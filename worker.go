package work

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/gomodule/redigo/redis"
)

const fetchKeysPerJobType = 6

var sleepBackoffs = []time.Duration{
	time.Millisecond * 0,
	time.Millisecond * 10,
	time.Millisecond * 100,
	time.Millisecond * 1000,
	time.Millisecond * 5000,
}

type worker struct {
	workerID    string
	poolID      string
	namespace   string
	pool        Pool
	jobTypes    map[string]*jobType
	middleware  []*middlewareHandler
	contextType reflect.Type

	redisFetchScript *redis.Script
	sampler          prioritySampler
	*observer

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

// Pool represents a pool of connections to a Redis server.
type Pool interface {
	Get() redis.Conn
}

func newWorker(namespace string, poolID string, pool Pool, contextType reflect.Type, middleware []*middlewareHandler, jobTypes map[string]*jobType) *worker {
	workerID := makeIdentifier()
	ob := newObserver(namespace, pool, workerID)

	w := &worker{
		workerID:    workerID,
		poolID:      poolID,
		namespace:   namespace,
		pool:        pool,
		contextType: contextType,

		observer: ob,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}

	w.updateMiddlewareAndJobTypes(middleware, jobTypes)

	return w
}

// note: can't be called while the thing is started
func (w *worker) updateMiddlewareAndJobTypes(middleware []*middlewareHandler, jobTypes map[string]*jobType) {
	w.middleware = middleware
	sampler := prioritySampler{}
	for _, jt := range jobTypes {
		sampler.add(jt.Priority,
			redisKeyJobs(w.namespace, jt.Name),
			redisKeyJobsInProgress(w.namespace, w.poolID, jt.Name),
			redisKeyJobsPaused(w.namespace, jt.Name),
			redisKeyJobsLock(w.namespace, jt.Name),
			redisKeyJobsLockInfo(w.namespace, jt.Name),
			redisKeyJobsConcurrency(w.namespace, jt.Name))
	}
	w.sampler = sampler
	w.jobTypes = jobTypes
	w.redisFetchScript = redis.NewScript(len(jobTypes)*fetchKeysPerJobType, redisLuaFetchJob)
}

func (w *worker) start() {
	go w.loop()
	go w.observer.start()
}

func (w *worker) stop() {
	w.stopChan <- struct{}{}
	<-w.doneStoppingChan
	w.observer.drain()
	w.observer.stop()
}

func (w *worker) drain() {
	w.drainChan <- struct{}{}
	<-w.doneDrainingChan
	w.observer.drain()
}

func (w *worker) loop() {
	var drained bool
	var consequtiveNoJobs int64

	// Begin immediately. We'll change the duration on each tick with a timer.Reset()
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-w.stopChan:
			w.doneStoppingChan <- struct{}{}
			return
		case <-w.drainChan:
			drained = true
			timer.Reset(0)
		case <-timer.C:
			job, err := w.fetchJob()
			if err != nil {
				logError("worker.fetch", err)
				timer.Reset(10 * time.Millisecond)
			} else if job != nil {
				w.processJob(job)
				consequtiveNoJobs = 0
				timer.Reset(0)
			} else {
				if drained {
					w.doneDrainingChan <- struct{}{}
					drained = false
				}
				consequtiveNoJobs++
				idx := consequtiveNoJobs
				if idx >= int64(len(sleepBackoffs)) {
					idx = int64(len(sleepBackoffs)) - 1
				}
				timer.Reset(sleepBackoffs[idx])
			}
		}
	}
}

func (w *worker) fetchJob() (*Job, error) {
	// resort queues
	// NOTE: we could optimize this to only resort every second, or something.
	w.sampler.sample()
	numKeys := len(w.sampler.samples) * fetchKeysPerJobType
	var scriptArgs = make([]interface{}, 0, numKeys+1)

	for _, s := range w.sampler.samples {
		scriptArgs = append(scriptArgs, s.redisJobs, s.redisJobsInProg, s.redisJobsPaused, s.redisJobsLock, s.redisJobsLockInfo, s.redisJobsMaxConcurrency) // KEYS[1-6 * N]
	}
	scriptArgs = append(scriptArgs, w.poolID) // ARGV[1]
	conn := w.pool.Get()
	defer conn.Close()

	values, err := redis.Values(w.redisFetchScript.Do(conn, scriptArgs...))
	if err == redis.ErrNil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	if len(values) != 3 {
		return nil, fmt.Errorf("need 3 elements back")
	}

	rawJSON, ok := values[0].([]byte)
	if !ok {
		return nil, fmt.Errorf("response msg not bytes")
	}

	dequeuedFrom, ok := values[1].([]byte)
	if !ok {
		return nil, fmt.Errorf("response queue not bytes")
	}

	inProgQueue, ok := values[2].([]byte)
	if !ok {
		return nil, fmt.Errorf("response in prog not bytes")
	}

	job, err := newJob(rawJSON, dequeuedFrom, inProgQueue)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (w *worker) processJob(job *Job) {
	if job.Unique {
		w.deleteUniqueJob(job)
	}

	var runErr error
	jt := w.jobTypes[job.Name]
	if jt == nil {
		runErr = fmt.Errorf("stray job: no handler")
		logError("process_job.stray", runErr)
	} else {
		w.observeStarted(job.Name, job.ID, job.Args)
		job.observer = w.observer // for Checkin
		_, runErr = runJob(job, w.contextType, w.middleware, jt)
		w.observeDone(job.Name, job.ID, runErr)
	}

	if runErr != nil {
		job.failed(runErr)
	}

	// Since we've taken the task and completed it, we must keep retrying commits
	// until we succeed, otherwise we'll end up with block job.
	retryErr(sleepBackoffs, func() error {
		err := w.removeJobFromInProgress(job, jt, runErr)
		if err != nil {
			logError("worker.remove_job_from_in_progress.lrem", err)
		}

		return err
	})
}

func (w *worker) deleteUniqueJob(job *Job) {
	uniqueKey, err := redisKeyUniqueJob(w.namespace, job.Name, job.Args)
	if err != nil {
		logError("worker.delete_unique_job.key", err)
		return
	}

	conn := w.pool.Get()
	defer conn.Close()

	_, err = conn.Do("DEL", uniqueKey)
	if err != nil {
		logError("worker.delete_unique_job.del", err)
	}
}

func (w *worker) removeJobFromInProgress(job *Job, jt *jobType, runErr error) error {
	var (
		forward          bool
		queue            string
		score            int64
		failedJobRawJSON []byte
	)

	if runErr != nil {
		switch {
		case jt != nil && jt.SkipDead:
			forward = false
		case jt != nil && int64(jt.MaxFails)-job.Fails > 0:
			forward = true
			queue = redisKeyRetry(w.namespace)
			score = nowEpochSeconds() + jt.calcBackoff(job)
		default:
			forward = true
			queue = redisKeyDead(w.namespace)
			score = nowEpochSeconds()
		}

		if forward {
			var err error
			failedJobRawJSON, err = job.serialize()
			if err != nil {
				logError("worker.removeJobFromInProgress.serialize", err)
				forward = false
			}
		}
	}

	conn := w.pool.Get()
	defer conn.Close()

	_, err := redisRemoveJobFromInProgress.Do(conn,
		job.inProgQueue,
		redisKeyJobsLock(w.namespace, job.Name),
		redisKeyJobsLockInfo(w.namespace, job.Name),
		queue,
		w.poolID,
		job.rawJSON,
		forward,
		score,
		failedJobRawJSON,
	)

	return err
}

// Default algorithm returns an fastly increasing backoff counter which grows in an unbounded fashion
func defaultBackoffCalculator(job *Job) int64 {
	fails := job.Fails
	return (fails * fails * fails * fails) + 15 + (rand.Int63n(30) * (fails + 1))
}

// retryErr retries fn until success.
func retryErr(backoffs []time.Duration, fn func() error) {
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			break
		}

		if len(backoffs) != 0 {
			idx := attempt
			if idx >= len(backoffs) {
				idx = len(backoffs) - 1
			}

			time.Sleep(backoffs[idx])
		}
	}
}
