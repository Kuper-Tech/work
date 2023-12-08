package work

import (
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeadPoolReaper(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	conn := pool.Get()
	defer conn.Close()

	workerPoolsKey := redisKeyWorkerPools(ns)

	// Create redis data
	var err error
	err = conn.Send("SADD", workerPoolsKey, "1")
	assert.NoError(t, err)
	err = conn.Send("SADD", workerPoolsKey, "2")
	assert.NoError(t, err)
	err = conn.Send("SADD", workerPoolsKey, "3")
	assert.NoError(t, err)

	err = conn.Send("HMSET", redisKeyHeartbeat(ns, "1"),
		"heartbeat_at", time.Now().Unix(),
		"job_names", "type1,type2",
	)
	assert.NoError(t, err)

	err = conn.Send("HMSET", redisKeyHeartbeat(ns, "2"),
		"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
		"job_names", "type1,type2",
	)
	assert.NoError(t, err)

	err = conn.Send("HMSET", redisKeyHeartbeat(ns, "3"),
		"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
		"job_names", "type1,type2",
	)
	assert.NoError(t, err)
	err = conn.Flush()
	assert.NoError(t, err)

	// Test getting dead pool
	reaper := newDeadPoolReaper(ns, pool, []string{}, 0, nil, noopLogger)
	deadPools, err := reaper.findDeadPools()
	assert.NoError(t, err)
	assert.Equal(t, poolsJobs{"2": {"type1", "type2"}, "3": {"type1", "type2"}}, deadPools)

	// Test requeueing jobs
	_, err = conn.Do("lpush", redisKeyJobsInProgress(ns, "2", "type1"), "foo")
	assert.NoError(t, err)
	_, err = conn.Do("incr", redisKeyJobsLock(ns, "type1"))
	assert.NoError(t, err)
	_, err = conn.Do("hincrby", redisKeyJobsLockInfo(ns, "type1"), "2", 1) // worker pool 2 has lock
	assert.NoError(t, err)

	// Ensure 0 jobs in jobs queue
	jobsCount, err := redis.Int(conn.Do("llen", redisKeyJobs(ns, "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)

	// Ensure 1 job in inprogress queue
	jobsCount, err = redis.Int(conn.Do("llen", redisKeyJobsInProgress(ns, "2", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Reap
	err = reaper.reap()
	assert.NoError(t, err)

	// Ensure 1 jobs in jobs queue
	jobsCount, err = redis.Int(conn.Do("llen", redisKeyJobs(ns, "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Ensure 0 job in inprogress queue
	jobsCount, err = redis.Int(conn.Do("llen", redisKeyJobsInProgress(ns, "2", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)

	// Locks should get cleaned up
	assert.EqualValues(t, 0, getInt64(pool, redisKeyJobsLock(ns, "type1")))
	v, _ := conn.Do("HGET", redisKeyJobsLockInfo(ns, "type1"), "2")
	assert.Nil(t, v)
}

func TestDeadPoolReaperNoHeartbeat(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"

	conn := pool.Get()
	defer conn.Close()

	workerPoolsKey := redisKeyWorkerPools(ns)

	// Create redis data
	var err error
	cleanKeyspace(ns, pool)
	err = conn.Send("SADD", workerPoolsKey, "1")
	assert.NoError(t, err)
	err = conn.Send("SADD", workerPoolsKey, "2")
	assert.NoError(t, err)
	err = conn.Send("SADD", workerPoolsKey, "3")
	assert.NoError(t, err)
	// stale lock info
	err = conn.Send("SET", redisKeyJobsLock(ns, "type1"), 3)
	assert.NoError(t, err)
	err = conn.Send("HSET", redisKeyJobsLockInfo(ns, "type1"), "1", 1)
	assert.NoError(t, err)
	err = conn.Send("HSET", redisKeyJobsLockInfo(ns, "type1"), "2", 1)
	assert.NoError(t, err)
	err = conn.Send("HSET", redisKeyJobsLockInfo(ns, "type1"), "3", 1)
	assert.NoError(t, err)
	err = conn.Flush()
	assert.NoError(t, err)

	// make sure test data was created
	numPools, err := redis.Int(conn.Do("scard", workerPoolsKey))
	assert.NoError(t, err)
	assert.EqualValues(t, 3, numPools)

	// Test getting dead pool ids
	reaper := newDeadPoolReaper(ns, pool, []string{"type1"}, 0, nil, noopLogger)
	deadPools, err := reaper.findDeadPools()
	assert.NoError(t, err)
	assert.Equal(t, poolsJobs{"1": nil, "2": nil, "3": nil}, deadPools)

	// Test requeueing jobs
	_, err = conn.Do("lpush", redisKeyJobsInProgress(ns, "2", "type1"), "foo")
	assert.NoError(t, err)

	// Ensure 0 jobs in jobs queue
	jobsCount, err := redis.Int(conn.Do("llen", redisKeyJobs(ns, "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)

	// Ensure 1 job in inprogress queue
	jobsCount, err = redis.Int(conn.Do("llen", redisKeyJobsInProgress(ns, "2", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Ensure dead worker pools still in the set
	jobsCount, err = redis.Int(conn.Do("scard", redisKeyWorkerPools(ns)))
	assert.NoError(t, err)
	assert.Equal(t, 3, jobsCount)

	// Reap
	err = reaper.reap()
	assert.NoError(t, err)

	// Ensure jobs queue was not altered
	jobsCount, err = redis.Int(conn.Do("llen", redisKeyJobs(ns, "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)

	// Ensure inprogress queue was not altered
	jobsCount, err = redis.Int(conn.Do("llen", redisKeyJobsInProgress(ns, "2", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Ensure dead worker pools were removed from the set
	jobsCount, err = redis.Int(conn.Do("scard", redisKeyWorkerPools(ns)))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)

	// Stale lock info was cleaned up using reap.curJobTypes
	assert.EqualValues(t, 0, getInt64(pool, redisKeyJobsLock(ns, "type1")))
	for _, poolID := range []string{"1", "2", "3"} {
		v, _ := conn.Do("HGET", redisKeyJobsLockInfo(ns, "type1"), poolID)
		assert.Nil(t, v)
	}
}

func TestDeadPoolReaperNoJobTypes(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	conn := pool.Get()
	defer conn.Close()

	workerPoolsKey := redisKeyWorkerPools(ns)

	// Create redis data
	var err error
	err = conn.Send("SADD", workerPoolsKey, "1")
	assert.NoError(t, err)
	err = conn.Send("SADD", workerPoolsKey, "2")
	assert.NoError(t, err)

	err = conn.Send("HMSET", redisKeyHeartbeat(ns, "1"),
		"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
	)
	assert.NoError(t, err)

	err = conn.Send("HMSET", redisKeyHeartbeat(ns, "2"),
		"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
		"job_names", "type1,type2",
	)
	assert.NoError(t, err)

	err = conn.Flush()
	assert.NoError(t, err)

	// Test getting dead pool
	reaper := newDeadPoolReaper(ns, pool, []string{}, 0, nil, noopLogger)
	deadPools, err := reaper.findDeadPools()
	assert.NoError(t, err)
	assert.Equal(t, poolsJobs{"2": {"type1", "type2"}}, deadPools)

	// Test requeueing jobs
	_, err = conn.Do("lpush", redisKeyJobsInProgress(ns, "1", "type1"), "foo")
	assert.NoError(t, err)
	_, err = conn.Do("lpush", redisKeyJobsInProgress(ns, "2", "type1"), "foo")
	assert.NoError(t, err)

	// Ensure 0 jobs in jobs queue
	jobsCount, err := redis.Int(conn.Do("llen", redisKeyJobs(ns, "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)

	// Ensure 1 job in inprogress queue for each job
	jobsCount, err = redis.Int(conn.Do("llen", redisKeyJobsInProgress(ns, "1", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)
	jobsCount, err = redis.Int(conn.Do("llen", redisKeyJobsInProgress(ns, "2", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Reap. Ensure job 2 is requeued but not job 1
	err = reaper.reap()
	assert.NoError(t, err)

	// Ensure 1 jobs in jobs queue
	jobsCount, err = redis.Int(conn.Do("llen", redisKeyJobs(ns, "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Ensure 1 job in inprogress queue for 1
	jobsCount, err = redis.Int(conn.Do("llen", redisKeyJobsInProgress(ns, "1", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Ensure 0 jobs in inprogress queue for 2
	jobsCount, err = redis.Int(conn.Do("llen", redisKeyJobsInProgress(ns, "2", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)
}

func TestDeadPoolReaperWithWorkerPools(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	job1 := "job1"
	stalePoolID := "aaa"
	cleanKeyspace(ns, pool)
	// test vars
	expectedDeadTime := time.Millisecond * 500

	// create a stale job with a heartbeat
	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("SADD", redisKeyWorkerPools(ns), stalePoolID)
	assert.NoError(t, err)
	_, err = conn.Do("LPUSH", redisKeyJobsInProgress(ns, stalePoolID, job1), `{"sleep": 10}`)
	assert.NoError(t, err)
	jobTypes := map[string]*jobType{"job1": nil}
	staleHeart := newWorkerPoolHeartbeater(ns, pool, stalePoolID, jobTypes, 1, []string{"id1"}, noopLogger)
	staleHeart.start()

	// heartbeat dispatched immediately but reaper waits for deadTime before first run
	time.Sleep(expectedDeadTime)

	// should have 1 stale job and empty job queue
	assert.EqualValues(t, 1, listSize(pool, redisKeyJobsInProgress(ns, stalePoolID, job1)))
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, job1)))

	// setup a worker pool and start the reaper, which should restart the stale job above
	wp := setupTestWorkerPool(pool, ns, job1, 1, JobOptions{Priority: 1})
	wp.deadPoolReaper = newDeadPoolReaper(wp.namespace, wp.pool, []string{"job1"}, 0, nil, noopLogger)
	wp.deadPoolReaper.deadTime = expectedDeadTime
	wp.deadPoolReaper.start()

	// sleep long enough for staleJob to be considered dead
	time.Sleep(expectedDeadTime * 2)

	// now we should have 1 job in queue and no more stale jobs
	assert.EqualValues(t, 1, listSize(pool, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
	staleHeart.stop()
	wp.deadPoolReaper.stop()
}

func TestDeadPoolReaperCleanStaleLocks(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	conn := pool.Get()
	defer conn.Close()
	job1, job2 := "type1", "type2"
	jobNames := []string{job1, job2}
	workerPoolID1, workerPoolID2 := "1", "2"
	lock1 := redisKeyJobsLock(ns, job1)
	lock2 := redisKeyJobsLock(ns, job2)
	lockInfo1 := redisKeyJobsLockInfo(ns, job1)
	lockInfo2 := redisKeyJobsLockInfo(ns, job2)

	// Create redis data
	var err error
	err = conn.Send("SET", lock1, 3)
	assert.NoError(t, err)
	err = conn.Send("SET", lock2, 1)
	assert.NoError(t, err)
	err = conn.Send("HSET", lockInfo1, workerPoolID1, 1) // workerPoolID1 holds 1 lock on job1
	assert.NoError(t, err)
	err = conn.Send("HSET", lockInfo1, workerPoolID2, 2) // workerPoolID2 holds 2 locks on job1
	assert.NoError(t, err)
	err = conn.Send("HSET", lockInfo2, workerPoolID2, 2) // test that we don't go below 0 on job2 lock
	assert.NoError(t, err)
	err = conn.Flush()
	assert.NoError(t, err)

	reaper := newDeadPoolReaper(ns, pool, jobNames, 0, nil, noopLogger)
	// clean lock info for workerPoolID1
	err = reaper.cleanStaleLockInfo(workerPoolID1, jobNames)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, getInt64(pool, lock1))   // job1 lock should be decr by 1
	assert.EqualValues(t, 1, getInt64(pool, lock2))   // job2 lock is unchanged
	v, _ := conn.Do("HGET", lockInfo1, workerPoolID1) // workerPoolID1 removed from job1's lock info
	assert.Nil(t, v)

	// now clean lock info for workerPoolID2
	err = reaper.cleanStaleLockInfo(workerPoolID2, jobNames)
	assert.NoError(t, err)
	// both locks should be at 0
	assert.EqualValues(t, 0, getInt64(pool, lock1))
	assert.EqualValues(t, 0, getInt64(pool, lock2))
	// worker pool ID 2 removed from both lock info hashes
	v, err = conn.Do("HGET", lockInfo1, workerPoolID2)
	assert.NoError(t, err)
	assert.Nil(t, v)
	v, err = conn.Do("HGET", lockInfo2, workerPoolID2)
	assert.NoError(t, err)
	assert.Nil(t, v)
}

func TestDeadPoolReaperTakeDeadPools(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	conn := pool.Get()
	defer conn.Close()

	workerPoolsKey := redisKeyWorkerPools(ns)

	// Create redis data
	var err error
	err = conn.Send("SADD", workerPoolsKey, "1")
	assert.NoError(t, err)
	err = conn.Send("SADD", workerPoolsKey, "2")
	assert.NoError(t, err)
	err = conn.Send("SADD", workerPoolsKey, "3")
	assert.NoError(t, err)

	err = conn.Send("HMSET", redisKeyHeartbeat(ns, "1"),
		"heartbeat_at", time.Now().Unix(),
		"job_names", "type1,type2",
	)
	assert.NoError(t, err)

	err = conn.Send("HMSET", redisKeyHeartbeat(ns, "2"),
		"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
		"job_names", "type1,type2",
	)
	assert.NoError(t, err)

	err = conn.Flush()
	assert.NoError(t, err)

	// Test getting dead pools
	reaper := newDeadPoolReaper(ns, pool, []string{}, 0, nil, noopLogger)
	deadPools, err := reaper.findDeadPools()
	assert.NoError(t, err)
	assert.Equal(t, poolsJobs{"2": {"type1", "type2"}, "3": nil}, deadPools)
}

func TestReaperLock(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	reaper := newDeadPoolReaper(ns, pool, []string{}, 0, nil, noopLogger)

	value, err := genValue()
	assert.NoError(t, err)

	acqired, err := reaper.acquireLock(value)
	assert.NoError(t, err)
	assert.True(t, acqired)

	acqired, err = reaper.acquireLock("aabbcc")
	assert.NoError(t, err)
	assert.False(t, acqired)

	conn := pool.Get()
	defer conn.Close()

	checkLock := func() {
		ttl, err := redis.Int(conn.Do("TTL", redisKeyReaperLock(ns)))
		assert.NoError(t, err)
		assert.Greater(t, ttl, 0)

		lvalue, err := redis.String(conn.Do("GET", redisKeyReaperLock(ns)))
		assert.NoError(t, err)
		assert.Equal(t, value, lvalue)
	}

	checkLock()

	err = reaper.releaseLock("aabbcc")
	assert.NoError(t, err)

	checkLock()

	err = reaper.releaseLock(value)
	assert.NoError(t, err)

	_, err = redis.String(conn.Do("GET", redisKeyReaperLock(ns)))
	assert.Error(t, err)
}

func TestDeadPoolReaperGetUnknownPools(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	workerPoolsKey := redisKeyWorkerPools(ns)
	workerPoolID1, workerPoolID2, workerPoolID3 := "1", "2", "3"

	job1, job2 := "type1", "type2"
	jobNames := []string{job1, job2}
	lockInfo1, lockInfo2 := redisKeyJobsLockInfo(ns, job1), redisKeyJobsLockInfo(ns, job2)

	conn := pool.Get()
	defer conn.Close()

	// Create redis data
	var err error
	err = conn.Send("SADD", workerPoolsKey, workerPoolID1)
	assert.NoError(t, err)

	err = conn.Send("HMSET", lockInfo1,
		workerPoolID1, 1, // workerPoolID1 holds 1 lock on job1
		workerPoolID2, 0, // unknown workerPoolID2 holds 0 locks on job1
		workerPoolID3, 2, // unknown workerPoolID3 holds 2 locks on job1
	)
	assert.NoError(t, err)

	err = conn.Send("HMSET", lockInfo2,
		workerPoolID1, 0, // workerPoolID1 holds 0 lock on job2
		workerPoolID2, 1, // unknown workerPoolID2 holds 1 locks on job2
		workerPoolID3, 1, // unknown workerPoolID3 holds 1 locks on job2
	)
	assert.NoError(t, err)

	assert.NoError(t, conn.Flush())

	// Run test
	reaper := newDeadPoolReaper(ns, pool, jobNames, 0, nil, noopLogger)
	unknownPools, err := reaper.getUnknownPools()
	assert.NoError(t, err)
	assert.Equal(t, poolsJobs{"2": {"type1", "type2"}, "3": {"type1", "type2"}}, unknownPools)
}

func TestDeadPoolReaperClearUnknownPool(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	workerPoolsKey := redisKeyWorkerPools(ns)
	workerPoolID1, workerPoolID2, workerPoolID3 := "1", "2", "3"

	job1, job2 := "type1", "type2"
	jobNames := []string{job1, job2}
	lock1, lock2 := redisKeyJobsLock(ns, job1), redisKeyJobsLock(ns, job2)
	lockInfo1, lockInfo2 := redisKeyJobsLockInfo(ns, job1), redisKeyJobsLockInfo(ns, job2)

	conn := pool.Get()
	defer conn.Close()

	// Create redis data
	var err error
	err = conn.Send("SADD", workerPoolsKey, workerPoolID1)
	assert.NoError(t, err)

	err = conn.Send("SET", lock1, 2)
	assert.NoError(t, err)

	err = conn.Send("HMSET", lockInfo1,
		workerPoolID1, 1, // workerPoolID1 holds 1 lock on job1
		workerPoolID2, 0, // unknown workerPoolID2 holds 0 locks on job1
		workerPoolID3, 1, // unknown workerPoolID3 holds 2 locks on job1
	)
	assert.NoError(t, err)

	err = conn.Send("LPUSH", redisKeyJobsInProgress(ns, workerPoolID1, job1), "foo")
	assert.NoError(t, err)

	err = conn.Send("LPUSH", redisKeyJobsInProgress(ns, workerPoolID3, job1), "bar")
	assert.NoError(t, err)

	err = conn.Send("SET", lock2, 2)
	assert.NoError(t, err)

	err = conn.Send("HMSET", lockInfo2,
		workerPoolID1, 0, // workerPoolID1 holds 0 lock on job2
		workerPoolID2, 2, // unknown workerPoolID2 holds 1 locks on job2
		workerPoolID3, 0, // unknown workerPoolID3 holds 1 locks on job2
	)
	assert.NoError(t, err)

	err = conn.Send("LPUSH", redisKeyJobsInProgress(ns, workerPoolID2, job2), "bar", "baz")
	assert.NoError(t, err)

	assert.NoError(t, conn.Flush())

	// Run test
	reaper := newDeadPoolReaper(ns, pool, jobNames, 0, nil, noopLogger)
	_, err = reaper.clearUnknownPools()
	assert.NoError(t, err)

	nLock1, err := redis.Int(conn.Do("GET", lock1))
	assert.NoError(t, err)
	assert.Equal(t, 1, nLock1)

	nLock2, err := redis.Int(conn.Do("GET", lock2))
	assert.NoError(t, err)
	assert.Equal(t, 0, nLock2)

	nLockInfo1, err := redis.StringMap(conn.Do("HGETALL", lockInfo1))
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{workerPoolID1: "1"}, nLockInfo1)

	nLockInfo2, err := redis.StringMap(conn.Do("HGETALL", lockInfo2))
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{workerPoolID1: "0"}, nLockInfo2)
}

func TestDeadPoolReaperRemoveDanglingLocks(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	workerPoolID1, workerPoolID2 := "1", "2"

	job1, job2, job3, job4 := "type1", "type2", "type3", "type4"
	jobNames := []string{job1, job2, job3, job4}
	lock1, lock2, lock3 := redisKeyJobsLock(ns, job1), redisKeyJobsLock(ns, job2), redisKeyJobsLock(ns, job3)
	lockInfo1, lockInfo2 := redisKeyJobsLockInfo(ns, job1), redisKeyJobsLockInfo(ns, job2)

	conn := pool.Get()
	defer conn.Close()

	// Create redis data
	var err error

	err = conn.Send("SET", lock1, 4) // One dangling lock
	assert.NoError(t, err)

	err = conn.Send("HMSET", lockInfo1,
		workerPoolID1, 2,
		workerPoolID2, 1,
	)
	assert.NoError(t, err)

	err = conn.Send("SET", lock2, 1) // No dangling locks
	assert.NoError(t, err)

	err = conn.Send("HMSET", lockInfo2,
		workerPoolID1, 0,
		workerPoolID2, 1,
	)
	assert.NoError(t, err)

	err = conn.Send("SET", lock3, 1) // One dangling lock
	assert.NoError(t, err)

	assert.NoError(t, conn.Flush())

	reaper := newDeadPoolReaper(ns, pool, jobNames, 0, nil, noopLogger)
	_, err = reaper.removeDanglingLocks()
	assert.NoError(t, err)

	// Checks
	nLock1, err := redis.Int(conn.Do("GET", lock1))
	assert.NoError(t, err)
	assert.Equal(t, 3, nLock1)

	nLock2, err := redis.Int(conn.Do("GET", lock2))
	assert.NoError(t, err)
	assert.Equal(t, 1, nLock2)

	nLock3, err := redis.Int(conn.Do("GET", lock3))
	assert.NoError(t, err)
	assert.Equal(t, 0, nLock3)
}

func TestDeadPoolReaperHook(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	workerPoolID1, workerPoolID2 := "1", "2"
	job1, job2 := "type1", "type2"
	jobNames := []string{job1, job2}
	lock2 := redisKeyJobsLock(ns, job2)
	lockInfo2 := redisKeyJobsLockInfo(ns, job2)

	conn := pool.Get()
	defer conn.Close()

	workerPoolsKey := redisKeyWorkerPools(ns)

	// Stale heartbeat
	var err error
	err = conn.Send("SADD", workerPoolsKey, workerPoolID1)
	assert.NoError(t, err)

	err = conn.Send("HMSET", redisKeyHeartbeat(ns, "1"),
		"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
		"job_names", job1,
	)
	assert.NoError(t, err)

	// Unknown pool and dangling lock
	err = conn.Send("SET", lock2, 2)
	assert.NoError(t, err)

	err = conn.Send("HMSET", lockInfo2,
		workerPoolID2, 1, // unknown workerPoolID2 holds 1 locks on job1
	)
	assert.NoError(t, err)

	assert.NoError(t, conn.Flush())

	noPoolHeartBeatJobs := []string{job1}
	unknownPoolJobs := []string{job2}
	danglingLockJobs := []string{job2}

	reaper := newDeadPoolReaper(ns, pool, jobNames, 0, func() func(ReapResult) {
		return func(rr ReapResult) {
			assert.NoError(t, rr.Err)
			assert.Equal(t, noPoolHeartBeatJobs, rr.NoPoolHeartBeatJobs)
			assert.Equal(t, unknownPoolJobs, rr.UnknownPoolJobs)
			assert.Equal(t, danglingLockJobs, rr.DanglingLockJobs)
		}
	}, noopLogger)
	require.NoError(t, reaper.reap())
}
