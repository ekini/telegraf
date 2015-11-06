/*
Logstream telegraf plugin

*/
package logstreamer

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/telegraf/plugins"
	"github.com/influxdb/telegraf/plugins/logstreamer/logstream"
)

// Lustre proc files can change between versions, so we want to future-proof
// by letting people choose what to look at.
type Logstreamer struct {
	filename string            `toml:-`
	Groups   []logstream.Group `toml:"group"`
	Dirs     []string
	Log      string
	Debug    bool
	metrics  chan logstream.Metric
	init     sync.Once
}

var sampleConfig = `
	#dirs = ["/var/log/nginx/"]
	#debug = false

	[[logstreamer.group]]
	mask = "^.*log$"
	rules = ['\s\[(?P<date>\d{1,2}/\w*/\d+:\d+:\d+:\d+ [+-]?\d+)\]\s.*?"\s(?P<code>\d{3})\s(?P<size_value>\d+)']
	name = "nginx"

	# Write the date below in your format
	# Mon Jan 2 15:04:05 -0700 MST 2006
	date_format = "02/Jan/2006:15:04:05 -0700"
`

// SampleConfig returns sample configuration message
func (l *Logstreamer) SampleConfig() string {
	return sampleConfig
}

// Description returns description of Lustre2 plugin
func (l *Logstreamer) Description() string {
	return "Gets metrics from local logs, parsing them using regular expressions"
}

// Gather reads stats from all lustre targets
func (l *Logstreamer) Gather(acc plugins.Accumulator) error {
	l.init.Do(func() {
		l.metrics = logstream.WatchFiles(l.Dirs, l.Groups, false, false)
	})
	timeout := time.After(1 * time.Second)
	errs := make([]string, 0)
	for {
		select {
		case <-timeout:
			goto out
		case metric := <-l.metrics:
			if err := l.addMetric(metric, acc); err != nil {
				errs = append(errs, err.Error())
			}
		}
	}
out:

	if len(errs) != 0 {
		return fmt.Errorf("Can't add metric: %s", strings.Join(errs, ", "))
	}

	return nil
}

func (l *Logstreamer) addMetric(metric logstream.Metric, acc plugins.Accumulator) (err error) {
	var date time.Time
	if !metric.Date.IsZero() { // compare with zero time
		date = metric.Date
	} else {
		date = time.Now()
	}
	tags := make(map[string]string)
	tags["group"] = metric.Group.Name

	values := make(map[string]interface{})

	for key, value := range metric.Match {
		key = strings.ToLower(key)
		if strings.HasSuffix(key, "_value") {
			// try to parse numerical value
			i, err := strconv.ParseInt(value, 10, 64)
			if err == nil {
				values[strings.TrimSuffix(key, "_value")] = i
				continue
			}
			f, err := strconv.ParseFloat(value, 64)
			if err == nil {
				values[strings.TrimSuffix(key, "_value")] = f
				continue
			}

			return fmt.Errorf("Can't parse '%s:%s' as numeric value", key, value, err)

		} else if strings.HasSuffix(key, "_string_value") {
			values[strings.TrimSuffix(key, "_string_value")] = value
		} else {
			tags[key] = value
		}
	}

	acc.AddFields(
		metric.Group.Name,
		values,
		tags,
		date,
	)

	return

}

func init() {
	plugins.Add("logstreamer", func() plugins.Plugin {
		return &Logstreamer{}
	})
}
