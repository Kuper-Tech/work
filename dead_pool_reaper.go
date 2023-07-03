package work

import (
	crand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"go.uber.org/multierr"
)

const (
	deadTime          = 10 * time.Second // 2 x heartbeat
	defaultReapPeriod = 5 * time.Minute
	reapJitterSecs    = 30
	requeueKeysPerJob = 4
)

type deadPoolReaper struct {
	namespace   string
	pool        Pool
	deadTime    time.Duration
	reapPeriod  time.Duration
	curJobTypes []string

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

func newDeadPoolReaper(namespace string, pool Pool, curJobTypes []string, reapPeriod time.Duration) *deadPoolReaper {
	if reapPeriod == 0 {
		reapPeriod = defaultReapPeriod
	}

	return &deadPoolReaper{
		namespace:        namespace,
		pool:             pool,
		deadTime:         deadTime,
		reapPeriod:       reapPeriod,
		curJobTypes:      curJobTypes,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}
}

func (r *deadPoolReaper) start() {
	go r.loop()
}

func (r *deadPoolReaper) stop() {
	r.stopChan <- struct{}{}
	<-r.doneStoppingChan
}

func (r *deadPoolReaper) loop() {
	Logger.Printf("Reaper: started with a period of %v", r.reapPeriod)

	// Reap immediately after we provide some time for initialization
	timer := time.NewTimer(r.deadTime)
	defer timer.Stop()

	for {
		select {
		case <-r.stopChan:
			r.doneStoppingChan <- struct{}{}
			return
		case <-timer.C:
			// Schedule next occurrence periodically with jitter
			timer.Reset(r.reapPeriod + time.Duration(rand.Intn(reapJitterSecs))*time.Second)

			if err := r.reap(); err != nil {
				logError("dead_pool_reaper.reap", err)
			}
		}
	}
}

func (r *deadPoolReaper) reap() (err error) {
	lockValue, err := genValue()
	if err != nil {
		return err
	}

	Logger.Printf("Reaper: trying to acquire lock...")

	acquired, err := r.acquireLock(lockValue)
	if err != nil {
		Logger.Printf("Reaper: acquiring lock: %v", err)
		return err
	}

	// Another reaper is already running
	if !acquired {
		Logger.Printf("Reaper: locked by another process")
		return nil
	}

	Logger.Printf("Reaper: lock is acquired")

	defer func() {
		err = r.releaseLock(lockValue)
	}()

	rErr := r.reapDeadPools()
	cErr := r.clearUnknownPools()

	// TODO: consider refactoring requeueInProgressJobs and cleanStaleLockInfo
	// and removing removeDanglingLocks. There was a block where lock is 1 and
	// lock_info is 0.
	dErr := r.removeDanglingLocks()

	return multierr.Combine(err, rErr, cErr, dErr)
}

// reapDeadPools collects the IDs of expired heartbeat pools and releases the
// associated resources.
func (r *deadPoolReaper) reapDeadPools() error {
	deadPoolIDs, err := r.findDeadPools()
	if err != nil {
		return err
	}

	Logger.Printf("Reaper: dead pools: %v", deadPoolIDs)

	conn := r.pool.Get()
	defer conn.Close()

	// Cleanup all dead pools
	for deadPoolID, jobTypes := range deadPoolIDs {
		lockJobTypes := jobTypes
		// if we found jobs from the heartbeat, requeue them and remove the heartbeat
		if len(jobTypes) > 0 {
			if err = r.requeueInProgressJobs(deadPoolID, jobTypes); err != nil {
				return err
			}

			if _, err = conn.Do("DEL", redisKeyHeartbeat(r.namespace, deadPoolID)); err != nil {
				return err
			}
		} else {
			// try to clean up locks for the current set of jobs if heartbeat was not found
			lockJobTypes = r.curJobTypes
		}

		// Cleanup any stale lock info
		if err = r.cleanStaleLockInfo(deadPoolID, lockJobTypes); err != nil {
			return err
		}

		// Remove dead pool from worker pools set
		if _, err = conn.Do("SREM", redisKeyWorkerPools(r.namespace), deadPoolID); err != nil {
			return err
		}
	}

	return nil
}

// clearUnknownPools enumerates the lock_info keys, collects pool IDs that are
// not in the worker_pools set, and releases associated locks.
func (r *deadPoolReaper) clearUnknownPools() error {
	unknownPools, err := r.getUnknownPools()
	if err != nil {
		return err
	}

	Logger.Printf("Reaper: unknown pools: %v", unknownPools)

	for poolID, jobTypes := range unknownPools {
		if err = r.requeueInProgressJobs(poolID, jobTypes); err != nil {
			return err
		}

		if err = r.cleanStaleLockInfo(poolID, jobTypes); err != nil {
			return err
		}
	}

	return nil
}

func (r *deadPoolReaper) cleanStaleLockInfo(poolID string, jobTypes []string) error {
	numKeys := len(jobTypes) * 2
	redisReapLocksScript := redis.NewScript(numKeys, redisLuaReapStaleLocks)
	var scriptArgs = make([]interface{}, 0, numKeys+1) // +1 for argv[1]

	for _, jobType := range jobTypes {
		scriptArgs = append(scriptArgs, redisKeyJobsLock(r.namespace, jobType), redisKeyJobsLockInfo(r.namespace, jobType))
	}
	scriptArgs = append(scriptArgs, poolID) // ARGV[1]

	conn := r.pool.Get()
	defer conn.Close()
	if _, err := redisReapLocksScript.Do(conn, scriptArgs...); err != nil {
		return err
	}

	negativeLocks, err := redis.Strings(redisReapLocksScript.Do(conn, scriptArgs...))
	if err != nil {
		return err
	}
	if len(negativeLocks) > 0 {
		Logger.Printf("Reaper: negative locks: %v", negativeLocks)
	}

	return nil
}

func (r *deadPoolReaper) requeueInProgressJobs(poolID string, jobTypes []string) error {
	numKeys := len(jobTypes) * requeueKeysPerJob
	redisRequeueScript := redis.NewScript(numKeys, redisLuaReenqueueJob)
	var scriptArgs = make([]interface{}, 0, numKeys+1)

	for _, jobType := range jobTypes {
		// pops from in progress, push into job queue and decrement the queue lock
		scriptArgs = append(scriptArgs, redisKeyJobsInProgress(r.namespace, poolID, jobType), redisKeyJobs(r.namespace, jobType), redisKeyJobsLock(r.namespace, jobType), redisKeyJobsLockInfo(r.namespace, jobType)) // KEYS[1-4 * N]
	}
	scriptArgs = append(scriptArgs, poolID) // ARGV[1]

	conn := r.pool.Get()
	defer conn.Close()

	// Keep moving jobs until all queues are empty
	for {
		values, err := redis.Values(redisRequeueScript.Do(conn, scriptArgs...))
		if err == redis.ErrNil {
			return nil
		} else if err != nil {
			return err
		}

		if len(values) != 3 {
			return fmt.Errorf("need 3 elements back")
		}
	}
}

// findDeadPools returns staled pools IDs and associated jobs.
func (r *deadPoolReaper) findDeadPools() (map[string][]string, error) {
	conn := r.pool.Get()
	defer conn.Close()

	workerPoolsKey := redisKeyWorkerPools(r.namespace)
	workerPoolIDs, err := redis.Strings(conn.Do("SMEMBERS", workerPoolsKey))
	if err != nil {
		return nil, err
	}

	deadPools := make(map[string][]string, len(workerPoolIDs))
	for _, workerPoolID := range workerPoolIDs {
		heartbeatKey := redisKeyHeartbeat(r.namespace, workerPoolID)
		heartbeatAt, err := redis.Int64(conn.Do("HGET", heartbeatKey, "heartbeat_at"))
		if err == redis.ErrNil {
			// heartbeat expired, save dead pool and use cur set of jobs from reaper
			deadPools[workerPoolID] = nil
			continue
		}
		if err != nil {
			return nil, err
		}

		// Check that last heartbeat was long enough ago to consider the pool dead
		if time.Unix(heartbeatAt, 0).Add(r.deadTime).After(time.Now()) {
			continue
		}

		jobTypesList, err := redis.String(conn.Do("HGET", heartbeatKey, "job_names"))
		if err == redis.ErrNil {
			continue
		}
		if err != nil {
			return nil, err
		}

		deadPools[workerPoolID] = strings.Split(jobTypesList, ",")
	}

	return deadPools, nil
}

// getUnknownPools returns the IDs of the unknown pools and associated job types
// found in the lock_info keys.
func (r *deadPoolReaper) getUnknownPools() (map[string][]string, error) {
	scriptArgs := make([]interface{}, 0, len(r.curJobTypes)+2) // +2 for keys count and pools key
	scriptArgs = append(scriptArgs, len(r.curJobTypes)+1)      // +1 for pools key
	scriptArgs = append(scriptArgs, redisKeyWorkerPools(r.namespace))

	for _, j := range r.curJobTypes {
		scriptArgs = append(scriptArgs, redisKeyJobsLockInfo(r.namespace, j))
	}

	conn := r.pool.Get()
	defer conn.Close()

	data, err := redis.Bytes(redisGetUnknownPoolsScript.Do(conn, scriptArgs...))
	if err != nil {
		return nil, err
	}

	var pools map[string][]string

	if err := json.Unmarshal(data, &pools); err != nil {
		return nil, err
	}

	// convert lock_info keys to job types
	for pool, keys := range pools {
		jobs := make([]string, 0, len(keys))

		for _, k := range keys {
			jobs = append(jobs, redisJobNameFromLockInfoKey(r.namespace, k))
		}

		pools[pool] = jobs
	}

	return pools, nil
}

// removeDanglingLocks adjusts the lock keys according to the lock_info numbers.
// TODO: it's better to find where the inconsistency comes from.
func (r *deadPoolReaper) removeDanglingLocks() error {
	keysCount := len(r.curJobTypes) * 2               // lock and lock_info keys
	scriptArgs := make([]interface{}, 0, keysCount+1) // +1 for keys count arg
	scriptArgs = append(scriptArgs, keysCount)

	for _, j := range r.curJobTypes {
		scriptArgs = append(scriptArgs, redisKeyJobsLock(r.namespace, j))
		scriptArgs = append(scriptArgs, redisKeyJobsLockInfo(r.namespace, j))
	}

	conn := r.pool.Get()
	defer conn.Close()

	keys, err := redis.Strings(redisRemoveDanglingLocksScript.Do(conn, scriptArgs...))
	if err != nil {
		return err
	}

	Logger.Printf("Reaper: dangling locks: %v", keys)

	return nil
}

// acquireLock acquires lock with a value and an expiration time for reap period.
func (r *deadPoolReaper) acquireLock(value string) (bool, error) {
	conn := r.pool.Get()
	defer conn.Close()

	reply, err := conn.Do(
		"SET", redisKeyReaperLock(r.namespace), value, "NX", "EX", int64(r.reapPeriod/time.Second))
	if err != nil {
		return false, err
	}

	return reply != nil, nil
}

// releaseLock releases lock with a value.
func (r *deadPoolReaper) releaseLock(value string) error {
	conn := r.pool.Get()
	defer conn.Close()

	_, err := redisReleaseLockScript.Do(conn, redisKeyReaperLock(r.namespace), value)

	return err
}

func genValue() (string, error) {
	b := make([]byte, 16)

	_, err := crand.Read(b)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b), nil
}
