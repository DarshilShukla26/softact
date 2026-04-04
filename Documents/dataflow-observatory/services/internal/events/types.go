package events

// RawEvent is the union schema for all event sources.
// Fields are pointers or interface{} to handle dirty/missing values gracefully.
type RawEvent struct {
	EventID   string `json:"event_id"`
	Source    string `json:"source"`
	Timestamp any    `json:"timestamp"` // may be string or int (dirty)

	// clickstream
	EventType *string `json:"event_type,omitempty"`
	UserID    *string `json:"user_id,omitempty"`
	SessionID *string `json:"session_id,omitempty"`
	URL       *string `json:"url,omitempty"`
	Referrer  *string `json:"referrer,omitempty"`
	ElementID *string `json:"element_id,omitempty"`
	IPAddress *string `json:"ip_address,omitempty"`
	Country   *string `json:"country,omitempty"`
	DurationMS any    `json:"duration_ms,omitempty"` // may be int or string (dirty)
	PageDepth  any    `json:"page_depth,omitempty"`  // may be float or out-of-range

	// iot_sensor
	SensorID     *string `json:"sensor_id,omitempty"`
	Location     *string `json:"location,omitempty"`
	Temperature  any     `json:"temperature,omitempty"`
	Humidity     any     `json:"humidity,omitempty"`
	BatteryLevel any     `json:"battery_level,omitempty"`
	Status       *string `json:"status,omitempty"`
	Firmware     *string `json:"firmware,omitempty"`

	// injected by simulator to tag dirty events
	Defect *string `json:"_defect,omitempty"`
}

// QualityAlert is published to the quality_alerts topic.
type QualityAlert struct {
	AlertID   string `json:"alert_id"`
	EventID   string `json:"event_id"`
	Source    string `json:"source"`
	Timestamp string `json:"timestamp"`
	AlertType string `json:"alert_type"` // schema_error | null_field | null_rate | anomaly | range_violation | type_error
	Field     string `json:"field,omitempty"`
	Message   string `json:"message"`
	Severity  string `json:"severity"` // low | medium | high | critical
	Value     any    `json:"value,omitempty"`
	Expected  string `json:"expected,omitempty"`
}

// SeverityScore maps severity labels to numeric scores for Redis sorted sets.
func SeverityScore(s string) float64 {
	switch s {
	case "critical":
		return 4
	case "high":
		return 3
	case "medium":
		return 2
	default:
		return 1
	}
}

// DeadLetterEvent wraps an unparseable raw message with metadata.
type DeadLetterEvent struct {
	OriginalPayload string `json:"original_payload"`
	Reason          string `json:"reason"`
	Timestamp       string `json:"timestamp"`
	Partition       int    `json:"partition"`
	Offset          int64  `json:"offset"`
}
