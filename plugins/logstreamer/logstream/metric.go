package logstream

import "time"

var metricsChan = make(chan Metric, 100)

func GetMetrics() chan Metric {
	return metricsChan
}

type Metric struct {
	Group *Group
	Match map[string]string
	Date  time.Time
}
