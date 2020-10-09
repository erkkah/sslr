//+build nothrottle

package sslr

func newThrottle(name string, percentage float64) *throttledOperation {
	return &throttledOperation{}
}

func (t *throttledOperation) start() {}

func (t *throttledOperation) end() {}

func (t *throttledOperation) wait() {}
