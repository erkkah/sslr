//+build !nothrottle

package sslr

import (
	"math"
	"time"

	"github.com/erkkah/letarette/pkg/logger"
)

func newThrottle(name string, percentage float64) *throttledOperation {
	logger.Debug.Printf("Created new throttle %q at %v%%", name, percentage)
	return &throttledOperation{
		name:  name,
		level: math.Max(1, math.Min(percentage, 100)) / 100,
	}
}

func (t *throttledOperation) start() {
	logger.Debug.Printf("Starting %s", t.name)
	t.jobStartTime = time.Now()
	if t.startTime.IsZero() {
		t.startTime = t.jobStartTime
	}
}

func (t *throttledOperation) end() {
	logger.Debug.Printf("Stopped %s", t.name)
	t.totalJobDuration += time.Since(t.jobStartTime)
}

func (t *throttledOperation) wait() {
	totalDuration := time.Since(t.startTime)
	utilization := float64(t.totalJobDuration.Milliseconds())
	logger.Debug.Printf("Utilization %.2f%%", 100*utilization/float64(totalDuration.Milliseconds()))
	if t.level >= 1 {
		return
	}

	utilizationLimit := float64(totalDuration.Milliseconds()) * t.level
	if utilization > utilizationLimit {
		waitTime := time.Duration(2*(utilization-utilizationLimit)) * time.Millisecond
		logger.Debug.Printf("Waiting %v to keep utilization at %.2f%%", waitTime, t.level*100)
		time.Sleep(waitTime)
	}
}
