package work

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCheckTimesHeap(t *testing.T) {
	t1, _ := time.Parse(time.RFC3339, "2019-01-01T00:00:00Z")
	t2, _ := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
	t3, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")

	tests := []struct {
		data     []time.Time
		expected []time.Time
	}{
		{
			[]time.Time{},
			[]time.Time{},
		},
		{
			[]time.Time{t1},
			[]time.Time{t1},
		},
		{
			[]time.Time{t1, t1},
			[]time.Time{t1},
		},
		{
			[]time.Time{t1, t2, t3},
			[]time.Time{t1, t2, t3},
		},
		{
			[]time.Time{t3, t3, t3},
			[]time.Time{t3},
		},
		{
			[]time.Time{t2, t1},
			[]time.Time{t1, t2},
		},
		{
			[]time.Time{t3, t2, t1},
			[]time.Time{t1, t2, t3},
		},
		{
			[]time.Time{t3, t3, t3, t2, t2, t1},
			[]time.Time{t1, t2, t3},
		},
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			h := newCheckTimesHeap()
			for _, v := range tt.data {
				h.Push(v)
			}
			require.Equal(t, len(tt.expected), h.Len())

			for i := 0; h.Len() > 0; i++ {
				tm := h.Pop()
				require.Equal(t, tt.expected[i], tm)
			}
		})
	}
}

func TestWatchdog(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	const jobName = "test"
	j, err := newPeriodicJob("* * * * * *", jobName)
	require.NoError(err)

	w := newWatchdog(
		watchdogWithFailCheckingTimeout(time.Millisecond * 2000),
	)
	defer w.stop()

	w.addPeriodicJobs(j)
	w.start()
	w.planning(time.Now())

	time.Sleep(time.Millisecond * 2000)
	w.processedJobs <- &Job{
		Name:       jobName,
		EnqueuedAt: time.Now().Unix(),
	}
	time.Sleep(time.Millisecond * 500)

	require.Equal(WatchdogStat{Name: "test", Processed: 1, Skipped: 0}, w.stats()[0])

	time.Sleep(time.Millisecond * 1600)
	require.Equal(WatchdogStat{Name: "test", Processed: 1, Skipped: 1}, w.stats()[0])
}

// A worker of another pod picked up the tick: the shared Redis marker exists, there is no local signal.
// The watchdog must not treat such a tick as skipped.
func TestWatchdogJobDoneByAnotherPod(t *testing.T) {
	require := require.New(t)

	const (
		ns      = "work-watchdog-done"
		jobName = "test"
		timeout = time.Second
	)

	pool := newTestPool(":6379")
	cleanKeyspace(ns, pool)
	defer cleanKeyspace(ns, pool)

	j, err := newPeriodicJob("* * * * * *", jobName)
	require.NoError(err)

	w := newWatchdog(
		watchdogWithFailCheckingTimeout(timeout),
		watchdogWithRedis(pool, ns),
	)
	w.addPeriodicJobs(j)

	now := time.Now()
	w.planning(now)
	planned := j.schedule.Next(now)

	// "Another pod" picked up the tick and set the shared marker.
	require.NoError(markPeriodicDone(pool, ns, jobName, planned.Unix()))

	w.checking(planned.Add(timeout + time.Second))

	require.Equal(WatchdogStat{Name: jobName, Processed: 1, Skipped: 0}, w.stats()[0])
}

// Nobody picked up the tick: there is no marker in Redis, so it is a real skip.
func TestWatchdogJobSkippedWithoutDoneMark(t *testing.T) {
	require := require.New(t)

	const (
		ns      = "work-watchdog-skipped"
		jobName = "test"
		timeout = time.Second
	)

	pool := newTestPool(":6379")
	cleanKeyspace(ns, pool)
	defer cleanKeyspace(ns, pool)

	j, err := newPeriodicJob("* * * * * *", jobName)
	require.NoError(err)

	w := newWatchdog(
		watchdogWithFailCheckingTimeout(timeout),
		watchdogWithRedis(pool, ns),
	)
	w.addPeriodicJobs(j)

	now := time.Now()
	w.planning(now)
	planned := j.schedule.Next(now)

	w.checking(planned.Add(timeout + time.Second))

	require.Equal(WatchdogStat{Name: jobName, Processed: 0, Skipped: 1}, w.stats()[0])
}

// The marker of one tick must not suppress the warning for another tick.
func TestWatchdogDoneMarkIsPerTick(t *testing.T) {
	require := require.New(t)

	const (
		ns      = "work-watchdog-per-tick"
		jobName = "test"
		timeout = time.Second
	)

	pool := newTestPool(":6379")
	cleanKeyspace(ns, pool)
	defer cleanKeyspace(ns, pool)

	j, err := newPeriodicJob("* * * * * *", jobName)
	require.NoError(err)

	w := newWatchdog(
		watchdogWithFailCheckingTimeout(timeout),
		watchdogWithRedis(pool, ns),
	)
	w.addPeriodicJobs(j)

	now := time.Now()
	w.planning(now)
	planned := j.schedule.Next(now)

	require.NoError(markPeriodicDone(pool, ns, jobName, planned.Unix()+1))

	w.checking(planned.Add(timeout + time.Second))

	require.Equal(WatchdogStat{Name: jobName, Processed: 0, Skipped: 1}, w.stats()[0])
}

// The worker sets the shared marker only for periodic jobs.
func TestWorkerMarksPeriodicJobDone(t *testing.T) {
	require := require.New(t)

	const (
		ns      = "work-worker-mark-done"
		jobName = "test"
	)

	pool := newTestPool(":6379")
	cleanKeyspace(ns, pool)
	defer cleanKeyspace(ns, pool)

	w := newWorker(ns, "1", pool, tstCtxType, nil, nil, noopLogger, make(chan *Job, 1))

	const epoch = int64(1700000000)

	w.markPeriodicJobDone(&Job{Name: jobName, ID: "regular-job-id", EnqueuedAt: epoch})
	done, err := periodicDone(pool, ns, jobName, epoch)
	require.NoError(err)
	require.False(done)

	// The requeuer overwrites EnqueuedAt with the requeue time, so the epoch is taken from the ID.
	w.markPeriodicJobDone(&Job{Name: jobName, ID: makeUniquePeriodicID(jobName, "* * * * * *", epoch), EnqueuedAt: epoch + 3})
	done, err = periodicDone(pool, ns, jobName, epoch)
	require.NoError(err)
	require.True(done)

	done, err = periodicDone(pool, ns, jobName, epoch+3)
	require.NoError(err)
	require.False(done)
}

func TestParsePeriodicEpoch(t *testing.T) {
	epoch, ok := parsePeriodicEpoch(makeUniquePeriodicID("job", "0 * * * * *", 1700000000))
	require.True(t, ok)
	require.Equal(t, int64(1700000000), epoch)

	// A ':' inside the spec or name does not matter: the last segment is used.
	epoch, ok = parsePeriodicEpoch(makeUniquePeriodicID("a:b", "*/5 * * * * *", 42))
	require.True(t, ok)
	require.Equal(t, int64(42), epoch)

	for _, id := range []string{"", "regular-job-id", "periodic:job:spec:notanumber"} {
		_, ok = parsePeriodicEpoch(id)
		require.False(t, ok, id)
	}
}

// End-to-end path: the tick sits in scheduled, the requeuer moves it to the queue and overwrites
// EnqueuedAt with the requeue time. The marker must still be set for the tick epoch.
func TestWatchdogJobDoneAfterRequeue(t *testing.T) {
	require := require.New(t)

	const (
		ns      = "work-watchdog-requeue"
		jobName = "test"
		spec    = "* * * * * *"
		timeout = time.Second
	)

	pool := newTestPool(":6379")
	cleanKeyspace(ns, pool)
	defer cleanKeyspace(ns, pool)

	j, err := newPeriodicJob(spec, jobName)
	require.NoError(err)

	w := newWatchdog(
		watchdogWithFailCheckingTimeout(timeout),
		watchdogWithRedis(pool, ns),
	)
	w.addPeriodicJobs(j)

	now := time.Now()
	w.planning(now)
	planned := j.schedule.Next(now)

	// A tick that the requeuer processes 3 seconds late.
	epoch := planned.Unix() - 3
	raw, err := (&Job{
		Name:             jobName,
		ID:               makeUniquePeriodicID(jobName, spec, epoch),
		EnqueuedAt:       epoch,
		StartingDeadline: epoch + 60,
	}).serialize()
	require.NoError(err)

	conn := pool.Get()
	_, err = conn.Do("ZADD", redisKeyScheduled(ns), epoch, raw)
	conn.Close()
	require.NoError(err)

	re := newRequeuer(ns, pool, redisKeyScheduled(ns), []string{jobName}, noopLogger)
	re.start()
	re.drain()
	re.stop()

	queued := jobOnQueue(pool, redisKeyJobs(ns, jobName))
	require.NotEqual(epoch, queued.EnqueuedAt, "requeuer should have overwritten EnqueuedAt")

	// The key must be set for the tick epoch, which is what the watchdog of another pod looks for.
	wk := newWorker(ns, "1", pool, tstCtxType, nil, nil, noopLogger, make(chan *Job, 1))
	wk.markPeriodicJobDone(queued)

	done, err := periodicDone(pool, ns, jobName, epoch)
	require.NoError(err)
	require.True(done)
}
