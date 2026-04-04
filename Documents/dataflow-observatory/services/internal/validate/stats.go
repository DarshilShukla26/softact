package validate

import (
	"math"
	"sync"
	"time"
)

// welford tracks an online mean and variance using Welford's algorithm.
// Safe for single-goroutine use; callers must hold appropriate locks.
type welford struct {
	n    int
	mean float64
	m2   float64
}

func (w *welford) update(x float64) {
	w.n++
	delta := x - w.mean
	w.mean += delta / float64(w.n)
	w.m2 += delta * (x - w.mean)
}

func (w *welford) stddev() float64 {
	if w.n < 2 {
		return 0
	}
	return math.Sqrt(w.m2 / float64(w.n-1))
}

// ZScore returns the absolute z-score of x against the current distribution,
// then updates the running stats with x.
func (w *welford) ZScore(x float64) float64 {
	sd := w.stddev()
	var z float64
	if sd > 0 {
		z = math.Abs(x-w.mean) / sd
	}
	w.update(x)
	return z
}

// AnomalyTracker maintains per-(source, field) Welford stats.
type AnomalyTracker struct {
	mu    sync.Mutex
	stats map[string]*welford
}

func NewAnomalyTracker() *AnomalyTracker {
	return &AnomalyTracker{stats: make(map[string]*welford)}
}

// Check returns the z-score for value and updates the running stats.
func (a *AnomalyTracker) Check(source, field string, value float64) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := source + ":" + field
	s, ok := a.stats[key]
	if !ok {
		s = &welford{}
		a.stats[key] = s
	}
	return s.ZScore(value)
}

// nullBucket counts events in a fixed time window.
type nullBucket struct {
	total  int
	nulls  int
	resetAt time.Time
}

func (b *nullBucket) rate() float64 {
	if b.total == 0 {
		return 0
	}
	return float64(b.nulls) / float64(b.total)
}

// NullRateTracker tracks null rates per (source, field) over a rolling window.
type NullRateTracker struct {
	mu      sync.Mutex
	buckets map[string]*nullBucket
	window  time.Duration
}

func NewNullRateTracker(window time.Duration) *NullRateTracker {
	return &NullRateTracker{
		buckets: make(map[string]*nullBucket),
		window:  window,
	}
}

// Record marks a field observation as null or non-null and returns the
// current null rate for the window. Returns -1 if window was just reset
// (too few observations to be meaningful).
func (t *NullRateTracker) Record(source, field string, isNull bool) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := source + ":" + field
	now := time.Now()
	b, ok := t.buckets[key]
	if !ok || now.After(b.resetAt) {
		b = &nullBucket{resetAt: now.Add(t.window)}
		t.buckets[key] = b
	}
	b.total++
	if isNull {
		b.nulls++
	}
	return b.rate()
}
