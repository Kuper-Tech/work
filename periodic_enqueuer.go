package work

import (
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/robfig/cron/v3"
)

const (
	periodicEnqueuerSleep   = 2 * time.Minute
	periodicEnqueuerHorizon = 4 * time.Minute
)

type periodicEnqueuer struct {
	namespace             string
	pool                  Pool
	periodicJobs          []*periodicJob
	scheduledPeriodicJobs []*scheduledPeriodicJob
	stopChan              chan struct{}
	doneStoppingChan      chan struct{}
	logger                StructuredLogger
}

type periodicJob struct {
	jobName  string
	spec     string
	schedule cron.Schedule
}

type scheduledPeriodicJob struct {
	scheduledAt      time.Time
	scheduledAtEpoch int64
	*periodicJob
}

func newPeriodicEnqueuer(
	namespace string,
	pool Pool,
	periodicJobs []*periodicJob,
	logger StructuredLogger,
) *periodicEnqueuer {
	return &periodicEnqueuer{
		namespace:        namespace,
		pool:             pool,
		periodicJobs:     periodicJobs,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
		logger:           logger,
	}
}

func (pe *periodicEnqueuer) start() {
	go pe.loop()
}

func (pe *periodicEnqueuer) stop() {
	pe.stopChan <- struct{}{}
	<-pe.doneStoppingChan
}

func (pe *periodicEnqueuer) loop() {
	// Begin reaping periodically
	timer := time.NewTimer(periodicEnqueuerSleep + time.Duration(rand.Intn(30))*time.Second)
	defer timer.Stop()

	if pe.shouldEnqueue() {
		err := pe.enqueue()
		if err != nil {
			pe.logger.Error("periodic_enqueuer.loop.enqueue", errAttr(err))
		}
	}

	for {
		select {
		case <-pe.stopChan:
			pe.doneStoppingChan <- struct{}{}
			return
		case t := <-timer.C:
			timer.Reset(periodicEnqueuerSleep + time.Duration(rand.Intn(30))*time.Second)
			shouldEnqueue := pe.shouldEnqueue()
			pe.logger.Debug("periodic_enqueuer.loop",
				slog.Time("enqueue_time", t),
				slog.Bool("should_enqueue", shouldEnqueue))
			if shouldEnqueue {
				err := pe.enqueue()
				if err != nil {
					pe.logger.Error("periodic_enqueuer.loop.enqueue", errAttr(err))
				}
			}
		}
	}
}

func (pe *periodicEnqueuer) enqueue() error {
	now := nowEpochSeconds()
	nowTime := time.Unix(now, 0)
	horizon := nowTime.Add(periodicEnqueuerHorizon)

	conn := pe.pool.Get()
	defer conn.Close()

	for _, pj := range pe.periodicJobs {
		for t := pj.schedule.Next(nowTime); t.Before(horizon); t = pj.schedule.Next(t) {
			epoch := t.Unix()
			id := makeUniquePeriodicID(pj.jobName, pj.spec, epoch)

			job := &Job{
				Name: pj.jobName,
				ID:   id,

				// This is technically wrong, but this lets the bytes be
				// identical for the same periodic job instance. If we don't do
				// this, we'd need to use a different approach -- probably
				// giving each periodic job its own history of the past 100
				// periodic jobs, and only scheduling a job if it's not in the
				// history.
				EnqueuedAt: epoch,
				Args:       nil,

				// Set the next activation time as the deadline for the current one.
				StartingDeadline: pj.schedule.Next(t).Unix(),
			}

			pe.logger.Debug("periodic_enqueuer.enqueue",
				slog.Time("job_scheduled_time", t),
				slog.String("job_name", pj.jobName),
				slog.String("job_id", id),
			)

			rawJSON, err := job.serialize()
			if err != nil {
				return err
			}

			_, err = conn.Do("ZADD", redisKeyScheduled(pe.namespace), epoch, rawJSON)
			if err != nil {
				return err
			}
		}
	}

	_, err := conn.Do("SET", redisKeyLastPeriodicEnqueue(pe.namespace), now)

	return err
}

func (pe *periodicEnqueuer) shouldEnqueue() bool {
	conn := pe.pool.Get()
	defer conn.Close()

	lastEnqueue, err := redis.Int64(conn.Do("GET", redisKeyLastPeriodicEnqueue(pe.namespace)))
	if err == redis.ErrNil {
		return true
	} else if err != nil {
		pe.logger.Error("periodic_enqueuer.should_enqueue", errAttr(err))
		return true
	}

	return lastEnqueue < (nowEpochSeconds() - int64(periodicEnqueuerSleep/time.Second))
}

func makeUniquePeriodicID(name, spec string, epoch int64) string {
	return fmt.Sprintf("periodic:%s:%s:%d", name, spec, epoch)
}
