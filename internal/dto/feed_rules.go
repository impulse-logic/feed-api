package dto

import "time"

// FieldError is one failed extra rule (checked after the binding tags), e.g. asn out of range.
// Handlers add it to the 422 "errors" map; the queues use the message as the dead-letter reason.
type FieldError struct {
	Field   string
	Message string
}

// timeLayouts: accepted date-time formats (deliveries eta, POS timestamp). ISO with a time zone is
// recommended (2026-10-20T08:00:00Z). The last four have no zone, so they use the app's local time
// zone (as the CLI used the server's).
var timeLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"2006-01-02",
}

// ParseTime tries every accepted layout.
func ParseTime(v string) (time.Time, bool) {
	for _, layout := range timeLayouts {
		if t, err := time.ParseInLocation(layout, v, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
