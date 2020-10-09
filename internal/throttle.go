package sslr

import (
	"time"
)

type throttledOperation struct {
	name             string
	level            float64
	startTime        time.Time
	totalJobDuration time.Duration
	jobStartTime     time.Time
}
