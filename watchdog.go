package work

import (
	"container/heap"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// WatchdogFailCheckingTimeout a default checking timeout that marks task as failed.
const WatchdogFailCheckingTimeout = 60 * time.Second

const processedJobsBuffer = 256

// The WatchdogStat struct represents statistics for a periodic jobs, including the name, counter,
type WatchdogStat struct {
	Name      string
	Processed int64
	Skipped   int64
}

// watchdog a struct that checks that periodic tasks are running.
// It is based on data about planned tasks and how they are actually processed.
type watchdog struct {
	periodicJobs        []*periodicJob
	jobs                map[string]*watchdogJob
	processedJobs       chan *Job
	failCheckingTimeout time.Duration
	stopChan            chan struct{}
	logger              StructuredLogger
}

type watchdogOption func(w *watchdog)

func watchdogWithLogger(logger StructuredLogger) watchdogOption {
	return func(w *watchdog) {
		w.logger = logger
	}
}

func watchdogWithFailCheckingTimeout(t time.Duration) watchdogOption {
	return func(w *watchdog) {
		w.failCheckingTimeout = t
	}
}

func newWatchdog(opts ...watchdogOption) *watchdog {
	w := &watchdog{
		jobs:          make(map[string]*watchdogJob),
		processedJobs: make(chan *Job, processedJobsBuffer),
		stopChan:      make(chan struct{}),
	}

	for _, opt := range opts {
		opt(w)
	}

	if w.logger == nil {
		w.logger = noopLogger
	}

	if w.failCheckingTimeout == 0 {
		w.failCheckingTimeout = WatchdogFailCheckingTimeout
	}

	return w
}

func (w *watchdog) addPeriodicJobs(jobs ...*periodicJob) {
	w.periodicJobs = append(w.periodicJobs, jobs...)
	for _, j := range w.periodicJobs {
		w.jobs[j.jobName] = &watchdogJob{
			checkTimes: newCheckTimesHeap(),
		}
	}
}

func (w *watchdog) start() {
	const checkTimeout = time.Second

	go func() {
		timer := time.NewTicker(checkTimeout)
		defer timer.Stop()

		for {
			select {
			case t := <-timer.C:
				w.planning(t)
				w.checking(t)
			case j := <-w.processedJobs:
				w.processed(j)
			case <-w.stopChan:
				return
			}
		}
	}()
}

func (w *watchdog) stop() {
	w.stopChan <- struct{}{}
}

// planning method is responsible for planning the execution of periodic jobs.
// It iterates over the list of periodic jobs, calculates the next scheduled time for each job
// based on the current time `t`, and updates the check list for each job with the new scheduled time.
func (w *watchdog) planning(t time.Time) {
	for _, j := range w.periodicJobs {
		n := j.schedule.Next(t)
		h := w.jobs[j.jobName].checkTimes
		if h.Push(n) {
			w.logger.Debug("Watchdog: planning job",
				slog.String("job_name", j.jobName),
				slog.Time("job_next_time", n),
				slog.Int("jobs_total_planned", h.Len()),
			)
		}
	}
}

// checking checks for skipped jobs based on the time `t`.
// It iterates over the scheduled times for each job and compares them with the
// current time plus the fail checking timeout. If a job's scheduled time has passed the fail checking
// timeout, it is considered as skipped, removed from the check list, and the `skip` method is called
// to increment the skipped count for that job.
func (w *watchdog) checking(t time.Time) {
	for name, job := range w.jobs {
		job.each(func(h *checkTimesHeap) bool {
			n, _ := h.Peek()
			if n.Add(w.failCheckingTimeout).Before(t) {
				h.Pop()
				job.skipped.Add(1)

				w.logger.Error("Watchdog: skipped job",
					slog.String("job_name", name),
					slog.Time("job_next_time", n),
					slog.Int64("jobs_skipped", job.skipped.Load()),
				)

				return false
			}

			return true
		})
	}
}

// processed method is responsible for handling a processed job in the watchdog system.
// It iterates over the scheduled times for each job and check if job was successfully processed.
func (w *watchdog) processed(j *Job) {
	job, ok := w.jobs[j.Name]
	if !ok {
		return
	}

	job.each(func(h *checkTimesHeap) bool {
		n, _ := h.Peek()
		if !n.After(time.Unix(j.EnqueuedAt, 0)) {
			h.Pop()
			job.processed.Add(1)

			w.logger.Debug("Watchdog: successfully processed job",
				slog.String("job_name", j.Name),
				slog.Time("job_next_time", n),
				slog.Int64("jobs_processed", job.processed.Load()),
			)
			return false
		}
		return true
	})
}

func (w *watchdog) stats() []WatchdogStat {
	res := make([]WatchdogStat, 0, len(w.jobs))

	for k, v := range w.jobs {
		res = append(res, WatchdogStat{
			Name:      k,
			Processed: v.processed.Load(),
			Skipped:   v.skipped.Load(),
		})
	}

	return res
}

type watchdogJob struct {
	checkTimes *checkTimesHeap
	processed  atomic.Int64
	skipped    atomic.Int64
}

func (w *watchdogJob) each(cb func(h *checkTimesHeap) bool) {
	h := w.checkTimes
	for h.Len() > 0 {
		if cb(h) {
			return
		}
	}
}

type checkTimes []time.Time

func (h checkTimes) Len() int           { return len(h) }
func (h checkTimes) Less(i, j int) bool { return h[i].Before(h[j]) }
func (h checkTimes) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *checkTimes) Push(x any) {
	*h = append(*h, x.(time.Time))
}

func (h *checkTimes) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type checkTimesHeap struct {
	mu         sync.RWMutex
	uniqTimes  map[time.Time]struct{}
	checkTimes *checkTimes
}

func newCheckTimesHeap() *checkTimesHeap {
	h := &checkTimes{}
	heap.Init(h)

	return &checkTimesHeap{
		uniqTimes:  make(map[time.Time]struct{}),
		checkTimes: h,
	}
}

func (h *checkTimesHeap) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.checkTimes.Len()
}

func (h *checkTimesHeap) Peek() (time.Time, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.checkTimes.Len() > 0 {
		return (*h.checkTimes)[0], true
	}
	return time.Time{}, false
}

func (h *checkTimesHeap) Push(t time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.uniqTimes[t]; exists {
		return false
	}

	h.uniqTimes[t] = struct{}{}
	heap.Push(h.checkTimes, t)

	return true
}

func (h *checkTimesHeap) Pop() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()

	t := heap.Pop(h.checkTimes).(time.Time)
	delete(h.uniqTimes, t)
	return t
}
