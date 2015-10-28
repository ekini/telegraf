package logstream

import (
	"regexp"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

type Group struct {
	Mask       *Mask
	Rules      []*Rule
	Name       string
	DateFormat string
}

type Mask struct {
	*regexp.Regexp
}

func (m *Mask) UnmarshalTOML(data []byte) (err error) {
	m.Regexp, err = regexp.Compile(strings.Trim(string(data), "\"'"))
	return err
}

func NewRule(sregexp string) (*Rule, error) {
	rule := &Rule{}
	var err error
	if rule.regexp, err = regexp.Compile(sregexp); err != nil {
		return rule, err
	}
	rule.subexpNames = rule.regexp.SubexpNames()
	return rule, nil
}

type Rule struct {
	regexp      *regexp.Regexp
	subexpNames []string
}

func (rule *Rule) UnmarshalTOML(data []byte) (err error) {
	rule.regexp, err = regexp.Compile(strings.Trim(string(data), "\"'"))
	if err == nil {
		rule.subexpNames = rule.regexp.SubexpNames()
	}
	return err
}

func (rule *Rule) Match(line *string) map[string]string {
	matches := rule.regexp.FindStringSubmatch(*line)
	if len(matches) == 0 {
		return nil
	}

	// TODO: cache subexnames
	out := make(map[string]string)
	for i, value := range matches[1:] {
		out[rule.subexpNames[i+1]] = value
	}
	if len(out) > 0 {
		return out
	}
	return nil
}

func (rule *Rule) process(group *Group, match map[string]string) {
	metric := Metric{Group: group}
	if group.DateFormat != "" {
		if rawDate, ok := match["date"]; ok {
			if date, err := time.Parse(group.DateFormat, rawDate); err == nil {
				delete(match, "date")
				metric.Date = date
			} else {
				log.Errorf("Can't parse date '%s': %s", rawDate, err)
				return
			}
		}
	}
	metric.Match = match
	metricsChan <- metric
}
