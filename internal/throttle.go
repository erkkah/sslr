package sslr

import (
	"math"
	"time"

	"github.com/erkkah/letarette/pkg/logger"
)

type throttledOperation struct {
	level            float64
	startTime        time.Time
	totalJobDuration time.Duration
	jobStartTime     time.Time
}

func newThrottle(percentage float64) *throttledOperation {
	return &throttledOperation{
		level: math.Max(1, math.Min(percentage, 100)) / 100,
	}
}

func (t *throttledOperation) start() {
	t.jobStartTime = time.Now()
	if t.startTime.IsZero() {
		t.startTime = t.jobStartTime
	}
}

func (t *throttledOperation) end() {
	t.totalJobDuration += time.Since(t.jobStartTime)
}

func (t *throttledOperation) wait() {
	totalDuration := time.Since(t.startTime)
	utilizationLimit := float64(totalDuration.Milliseconds()) * t.level
	utilization := float64(t.totalJobDuration.Milliseconds())
	logger.Debug.Printf("Utilization %.2f%%", 100*t.level*utilization/utilizationLimit)
	if utilization > utilizationLimit {
		waitTime := time.Duration(2*(utilization-utilizationLimit)) * time.Millisecond
		logger.Debug.Printf("Waiting %v to keep utilization at %.2f%%", waitTime, t.level*100)
		time.Sleep(waitTime)
	}
}
