package jobq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

type JobFunc func(ctx context.Context) (interface{}, error)

type JobQueue struct {
	l    *sync.RWMutex
	c    *Config
	jobs map[string]*Job
}

type Job struct {
	ID         string
	Status     string
	Expiration time.Time
	Error      error
	output     interface{}
	cancel     context.CancelFunc
}

type Config struct {
	ExpireDuration time.Duration
}

func WithExpireDuration(duration time.Duration) Config {
	return Config{ExpireDuration: duration}
}

func NewJobQueue(configs ...Config) *JobQueue {
	// Initilize a new job queue
	jq := &JobQueue{
		jobs: map[string]*Job{},
		l:    &sync.RWMutex{},
		// Set default configuration
		c: &Config{
			// Default 10min expireation duration for a job
			ExpireDuration: time.Minute * 10,
		},
	}
	// Apply configurations to jq object
	applyConfig(jq, configs)
	return jq
}

func (jq *JobQueue) Run(fn JobFunc) (jobID string) {
	jobID = hash(fmt.Sprintf("%d", time.Now().UnixMilli()))
	ctx, cancel := context.WithCancel(context.Background())

	job := &Job{
		ID:     jobID,
		Status: "running",
		cancel: cancel,
	}

	jq.l.Lock()
	jq.jobs[jobID] = job
	jq.l.Unlock()
	go func() {
		out, err := fn(ctx)
		if err != nil {
			job.Error = err
		} else {
			job.output = out
		}
		job.Status = "finished"
		// Set 10Min for expiration
		job.Expiration = time.Now().Add(time.Minute * 10)
	}()

	return jobID
}

func (jq *JobQueue) Get(jobID string) (*Job, error) {
	jq.cleanExpired()

	jq.l.RLock()
	defer jq.l.RUnlock()
	if j, ok := jq.jobs[jobID]; ok {
		return j, nil
	}

	return nil, fmt.Errorf("job %s not found or expired", jobID)
}

func (j *Job) Output() interface{} {
	return j.output
}

// =================================================================

func hash(str string) string {
	id := sha256.Sum256([]byte(str))
	return hex.EncodeToString(id[:])[:6]
}

func applyConfig(jq *JobQueue, configs []Config) {
	for _, c := range configs {
		if c.ExpireDuration.Seconds() != 0 {
			jq.c.ExpireDuration = c.ExpireDuration
		}
	}
}

func (jq *JobQueue) cleanExpired() {
	jq.l.Lock()
	defer jq.l.Unlock()

	deleteIds := []string{}
	for _, j := range jq.jobs {
		if time.Now().After(j.Expiration) {
			deleteIds = append(deleteIds, j.ID)
		}
	}
	for _, id := range deleteIds {
		delete(jq.jobs, id)
	}
}
