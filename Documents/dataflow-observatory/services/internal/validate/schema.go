package validate

import (
	"fmt"
	"time"

	"dataflow-observatory/services/internal/events"

	"github.com/google/uuid"
)

// requiredFields lists mandatory fields per source type.
var requiredFields = map[string][]string{
	"clickstream": {"event_id", "source", "timestamp", "user_id", "event_type", "url"},
	"iot_sensor":  {"event_id", "source", "timestamp", "sensor_id", "temperature", "humidity"},
}

// numericRanges defines [min, max] for fields that must be bounded.
var numericRanges = map[string][2]float64{
	"temperature":   {-50, 80},
	"humidity":      {0, 100},
	"battery_level": {0, 100},
	"page_depth":    {0, 1},
	"duration_ms":   {0, 300_000},
}

// Validator encapsulates all stateful DQ checks.
type Validator struct {
	anomaly   *AnomalyTracker
	nullRates *NullRateTracker

	NullThreshold   float64
	ZScoreThreshold float64
}

func New(nullThreshold, zScoreThreshold float64, windowSeconds int) *Validator {
	return &Validator{
		anomaly:         NewAnomalyTracker(),
		nullRates:       NewNullRateTracker(time.Duration(windowSeconds) * time.Second),
		NullThreshold:   nullThreshold,
		ZScoreThreshold: zScoreThreshold,
	}
}

// Check runs all DQ rules against ev and returns any alerts generated.
func (v *Validator) Check(ev *events.RawEvent) []*events.QualityAlert {
	var alerts []*events.QualityAlert
	add := func(a *events.QualityAlert) { alerts = append(alerts, a) }

	source := ev.Source
	now := time.Now().UTC().Format(time.RFC3339)

	// ── 1. Required field presence ────────────────────────────────────────────
	fields, known := requiredFields[source]
	if !known {
		add(newAlert(ev, now, "schema_error", "", "critical",
			fmt.Sprintf("unknown source type %q", source), nil, ""))
	} else {
		fieldMap := eventFieldMap(ev)
		for _, f := range fields {
			val, exists := fieldMap[f]
			rate := v.nullRates.Record(source, f, !exists || val == nil)
			if !exists || val == nil {
				add(newAlert(ev, now, "null_field", f, "high",
					fmt.Sprintf("required field %q is null or missing", f), val, "non-null"))
			}
			// ── 2. Null rate threshold ─────────────────────────────────────
			if rate > v.NullThreshold {
				add(newAlert(ev, now, "null_rate", f, "medium",
					fmt.Sprintf("null rate %.1f%% exceeds threshold %.1f%% for field %q",
						rate*100, v.NullThreshold*100, f),
					rate, fmt.Sprintf("<= %.0f%%", v.NullThreshold*100)))
			}
		}
	}

	// ── 3. Range checks + anomaly detection on numeric fields ─────────────────
	numericFields := numericFieldMap(ev)
	for field, val := range numericFields {
		// range check
		if bounds, ok := numericRanges[field]; ok {
			if val < bounds[0] || val > bounds[1] {
				add(newAlert(ev, now, "range_violation", field, "high",
					fmt.Sprintf("%s=%.2f is outside allowed range [%.0f, %.0f]",
						field, val, bounds[0], bounds[1]),
					val, fmt.Sprintf("[%.0f, %.0f]", bounds[0], bounds[1])))
			}
		}
		// anomaly detection — only meaningful after a warm-up period
		z := v.anomaly.Check(source, field, val)
		if z > v.ZScoreThreshold {
			add(newAlert(ev, now, "anomaly", field, "medium",
				fmt.Sprintf("%s z-score=%.2f exceeds threshold %.1f", field, z, v.ZScoreThreshold),
				val, fmt.Sprintf("z <= %.1f", v.ZScoreThreshold)))
		}
	}

	// ── 4. Timestamp format check ─────────────────────────────────────────────
	switch ts := ev.Timestamp.(type) {
	case string:
		if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
			if _, err2 := time.Parse(time.RFC3339, ts); err2 != nil {
				add(newAlert(ev, now, "type_error", "timestamp", "medium",
					fmt.Sprintf("timestamp %q is not a valid ISO-8601 string", ts),
					ts, "RFC3339"))
			}
		}
	case float64: // JSON numbers arrive as float64
		add(newAlert(ev, now, "type_error", "timestamp", "medium",
			"timestamp is a numeric epoch instead of ISO-8601 string",
			ts, "RFC3339 string"))
	case nil:
		add(newAlert(ev, now, "null_field", "timestamp", "high",
			"timestamp is null", nil, "RFC3339 string"))
	}

	return alerts
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newAlert(ev *events.RawEvent, ts, alertType, field, severity, msg string,
	value any, expected string) *events.QualityAlert {
	return &events.QualityAlert{
		AlertID:   uuid.NewString(),
		EventID:   ev.EventID,
		Source:    ev.Source,
		Timestamp: ts,
		AlertType: alertType,
		Field:     field,
		Severity:  severity,
		Message:   msg,
		Value:     value,
		Expected:  expected,
	}
}

// eventFieldMap returns a flat map of field name → value for presence checks.
func eventFieldMap(ev *events.RawEvent) map[string]any {
	m := map[string]any{
		"event_id":      ev.EventID,
		"source":        ev.Source,
		"timestamp":     ev.Timestamp,
		"event_type":    ptrVal(ev.EventType),
		"user_id":       ptrVal(ev.UserID),
		"session_id":    ptrVal(ev.SessionID),
		"url":           ptrVal(ev.URL),
		"sensor_id":     ptrVal(ev.SensorID),
		"location":      ptrVal(ev.Location),
		"temperature":   ev.Temperature,
		"humidity":      ev.Humidity,
		"battery_level": ev.BatteryLevel,
	}
	return m
}

func ptrVal(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

// numericFieldMap extracts numeric values from the event for range/anomaly checks.
func numericFieldMap(ev *events.RawEvent) map[string]float64 {
	m := make(map[string]float64)
	tryFloat := func(name string, v any) {
		switch x := v.(type) {
		case float64:
			m[name] = x
		case int:
			m[name] = float64(x)
		}
	}
	tryFloat("temperature", ev.Temperature)
	tryFloat("humidity", ev.Humidity)
	tryFloat("battery_level", ev.BatteryLevel)
	tryFloat("duration_ms", ev.DurationMS)
	tryFloat("page_depth", ev.PageDepth)
	return m
}
