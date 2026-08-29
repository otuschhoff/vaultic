package maintenance

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/otuschhoff/vaultic/internal/index/daemon"
	"github.com/otuschhoff/vaultic/internal/index/schema"
)

// SeriesOptions selects a metric, a bucket width, and a range over the pack
// history rollups.
type SeriesOptions struct {
	Metric string
	Bucket string
	Since  time.Time
	Until  time.Time
	// GroupBy breaks the series down by "type"; empty reports one series.
	GroupBy         string
	Histogram       bool
	Forecast        bool
	AllowIncomplete bool
	// Now anchors forecasting and is overridable for tests.
	Now time.Time
}

// SeriesPoint is one bucket of a time series.
type SeriesPoint struct {
	BucketStart int64  `json:"bucket_start"`
	Group       string `json:"group,omitempty"`
	Value       int64  `json:"value"`
	// Coverage is the weakest coverage of any bucket folded into this point,
	// so a point is never reported as more trustworthy than its inputs.
	Coverage       string `json:"coverage"`
	EventsObserved uint64 `json:"events_observed"`
}

// ForecastResult projects a series forward by least-squares fit.
type ForecastResult struct {
	PerBucket        float64 `json:"per_bucket"`
	NextBucketValue  float64 `json:"next_bucket_value"`
	BucketsUsed      uint64  `json:"buckets_used"`
	IncompleteInput  bool    `json:"incomplete_input"`
	RefusedReason    string  `json:"refused_reason,omitempty"`
	ProjectionAmount int64   `json:"projection_amount"`
}

// HistogramBin counts buckets whose value falls in a range, which is how a
// distribution of pack creation or change activity is rendered.
type HistogramBin struct {
	LowerBound int64  `json:"lower_bound"`
	UpperBound int64  `json:"upper_bound"`
	Count      uint64 `json:"count"`
}

// SeriesResult is the versioned JSON contract of `index history`.
type SeriesResult struct {
	SchemaVersion int           `json:"schema_version"`
	Metric        string        `json:"metric"`
	Bucket        string        `json:"bucket"`
	Points        []SeriesPoint `json:"points"`

	// Repack and promotion churn are always reported separately from growth,
	// because a rewrite is neither new data nor deletion.
	BytesRepacked uint64 `json:"bytes_repacked"`
	PacksRepacked uint64 `json:"packs_repacked"`
	BytesPromoted uint64 `json:"bytes_promoted"`
	PacksPromoted uint64 `json:"packs_promoted"`

	CompleteBuckets      uint64 `json:"complete_buckets"`
	PartialBuckets       uint64 `json:"partial_buckets"`
	ReconstructedBuckets uint64 `json:"reconstructed_buckets"`

	Histogram []HistogramBin  `json:"histogram,omitempty"`
	Forecast  *ForecastResult `json:"forecast,omitempty"`
}

// HasIncompleteCoverage reports whether any point folded in a bucket that was
// not fully observed.
func (result SeriesResult) HasIncompleteCoverage() bool {
	return result.PartialBuckets != 0 || result.ReconstructedBuckets != 0
}

var seriesMetrics = map[string]struct{}{
	"packs": {}, "bytes": {}, "created": {}, "deleted": {},
	"repacked": {}, "promoted": {}, "net-growth": {}, "unused": {},
}

// HistorySeries builds a time series from the rollup buckets.
//
// Weekly is derived from daily buckets rather than stored, because a week is a
// presentation choice while the stored granularities are hourly, daily, and
// monthly.
func HistorySeries(ctx context.Context, store Store, options SeriesOptions) (SeriesResult, error) {
	result := SeriesResult{SchemaVersion: IntrospectSchemaVersion, Metric: options.Metric, Bucket: options.Bucket}
	if _, ok := seriesMetrics[options.Metric]; !ok {
		return result, fmt.Errorf("unsupported metric %q; supported: packs, bytes, created, deleted, repacked, promoted, net-growth, unused", options.Metric)
	}
	if options.GroupBy != "" && options.GroupBy != "type" {
		return result, fmt.Errorf("unsupported grouping %q; supported: type", options.GroupBy)
	}
	source, weekly, err := seriesGranularity(options.Bucket)
	if err != nil {
		return result, err
	}

	type pointKey struct {
		start uint64
		group string
	}
	folded := map[pointKey]*SeriesPoint{}
	// Rollup buckets are written by `index history prune`. Raw events that
	// have not been rolled up yet are folded in here on the fly, so history is
	// answerable without first mutating the repository: an operator asking a
	// question must not have to run a maintenance command to get an answer.
	seen := map[bucketKeyOf]bool{}
	fold := func(rawKey bucketKeyOf, bucket schema.PackHistoryBucket) {
		if !withinRange(rawKey.start, options.Since, options.Until) {
			return
		}
		start := rawKey.start
		if weekly {
			start = weekStart(rawKey.start)
		}
		group := ""
		if options.GroupBy == "type" {
			group = packTypeName(rawKey.packType)
		}
		key := pointKey{start: start, group: group}
		point, ok := folded[key]
		if !ok {
			point = &SeriesPoint{BucketStart: int64(start), Group: group, Coverage: schema.CoverageComplete.String()}
			folded[key] = point
		}
		point.Value += metricValue(options.Metric, bucket)
		point.EventsObserved += bucket.EventsObserved
		point.Coverage = weakestCoverage(point.Coverage, bucket.Coverage.String())

		result.BytesRepacked += bucket.BytesRepacked
		result.PacksRepacked += bucket.PacksRepacked
		result.BytesPromoted += bucket.BytesPromoted
		result.PacksPromoted += bucket.PacksPromoted
	}

	err = scan(ctx, store, schema.PackHistoryBucketPrefix(source), func(entry daemon.KeyValue) error {
		parsed, parseErr := schema.ParseKey(entry.Key)
		if parseErr != nil || parsed.Kind != schema.KeyPackHistoryBucket {
			return nil
		}
		bucket, decodeErr := schema.UnmarshalPackHistoryBucket(entry.Value)
		if decodeErr != nil {
			// A corrupt bucket is skipped: history is advisory.
			return nil
		}
		rawKey := bucketKeyOf{
			granularity: source, start: parsed.EventTime,
			backend: parsed.Backend, packType: parsed.PackType,
		}
		seen[rawKey] = true
		fold(rawKey, bucket)
		return nil
	})
	if err != nil {
		return result, err
	}

	if err := foldPendingRawEvents(ctx, store, source, seen, fold); err != nil {
		return result, err
	}

	points := make([]SeriesPoint, 0, len(folded))
	for _, point := range folded {
		points = append(points, *point)
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].BucketStart != points[j].BucketStart {
			return points[i].BucketStart < points[j].BucketStart
		}
		return points[i].Group < points[j].Group
	})
	result.Points = points
	for _, point := range points {
		switch point.Coverage {
		case schema.CoverageComplete.String():
			result.CompleteBuckets++
		case schema.CoveragePartial.String():
			result.PartialBuckets++
		case schema.CoverageReconstructed.String():
			result.ReconstructedBuckets++
		}
	}

	if options.Histogram {
		result.Histogram = buildHistogram(points)
	}
	if options.Forecast {
		result.Forecast = forecastSeries(points, options.AllowIncomplete)
	}
	return result, nil
}

// foldPendingRawEvents accumulates raw history events that no rollup bucket
// covers yet and folds them into the series. A bucket that already exists on
// disk wins, because the stored bucket may summarise events that retention has
// since discarded, whereas the raw events only describe what is still present.
func foldPendingRawEvents(ctx context.Context, store Store, granularity schema.HistoryGranularity, seen map[bucketKeyOf]bool, fold func(bucketKeyOf, schema.PackHistoryBucket)) error {
	scanned, err := ScanHistory(ctx, store, 0, 0)
	if err != nil {
		return err
	}
	if len(scanned.Events) == 0 {
		return nil
	}
	rawFloor, err := readHistoryMarker(ctx, store, schema.HistoryRawFloorKey())
	if err != nil {
		return err
	}
	enabledAt, err := readHistoryMarker(ctx, store, schema.HistoryEnabledAtKey())
	if err != nil {
		return err
	}
	accumulated := AccumulateBuckets(scanned.Events, []schema.HistoryGranularity{granularity}, rawFloor, enabledAt)
	for key, bucket := range accumulated {
		if seen[key] {
			continue
		}
		fold(key, bucket)
	}
	return nil
}

func seriesGranularity(bucket string) (schema.HistoryGranularity, bool, error) {
	switch strings.ToLower(bucket) {
	case "hour":
		return schema.GranularityHourly, false, nil
	case "day":
		return schema.GranularityDaily, false, nil
	case "week":
		// Derived from daily buckets; there is no stored weekly granularity.
		return schema.GranularityDaily, true, nil
	case "month":
		return schema.GranularityMonthly, false, nil
	}
	return 0, false, fmt.Errorf("unsupported bucket %q; supported: hour, day, week, month", bucket)
}

// weekStart truncates to the ISO-8601 week (Monday) in UTC. Using UTC keeps a
// boundary stable regardless of the reader's timezone or any DST transition in
// it; a local-time week would silently produce 23- and 25-hour weeks.
func weekStart(unixSeconds uint64) uint64 {
	moment := time.Unix(int64(unixSeconds), 0).UTC()
	weekday := (int(moment.Weekday()) + 6) % 7 // Monday = 0
	monday := time.Date(moment.Year(), moment.Month(), moment.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -weekday)
	return uint64(monday.Unix())
}

func withinRange(start uint64, since, until time.Time) bool {
	if !since.IsZero() && int64(start) < since.UTC().Unix() {
		return false
	}
	if !until.IsZero() && int64(start) >= until.UTC().Unix() {
		return false
	}
	return true
}

func metricValue(metric string, bucket schema.PackHistoryBucket) int64 {
	switch metric {
	case "packs":
		return int64(bucket.PacksCreated) - int64(bucket.PacksDeleted)
	case "bytes":
		return int64(bucket.BytesAdded) - int64(bucket.BytesDeleted)
	case "created":
		return int64(bucket.PacksCreated)
	case "deleted":
		return int64(bucket.PacksDeleted)
	case "repacked":
		return int64(bucket.BytesRepacked)
	case "promoted":
		return int64(bucket.BytesPromoted)
	case "net-growth":
		// Repacked and promoted bytes are deliberately excluded: rewriting or
		// moving data is not growth.
		return int64(bucket.BytesAdded) - int64(bucket.BytesDeleted)
	case "unused":
		return int64(bucket.EndPayloadSize)
	}
	return 0
}

// weakestCoverage keeps a folded point honest: combining a complete bucket with
// a reconstructed one yields reconstructed.
func weakestCoverage(left, right string) string {
	rank := map[string]int{
		schema.CoverageComplete.String():      0,
		schema.CoveragePartial.String():       1,
		schema.CoverageReconstructed.String(): 2,
	}
	if rank[right] > rank[left] {
		return right
	}
	return left
}

func buildHistogram(points []SeriesPoint) []HistogramBin {
	if len(points) == 0 {
		return nil
	}
	lowest, highest := points[0].Value, points[0].Value
	for _, point := range points {
		lowest = min(lowest, point.Value)
		highest = max(highest, point.Value)
	}
	const bins = 10
	if lowest == highest {
		return []HistogramBin{{LowerBound: lowest, UpperBound: highest, Count: uint64(len(points))}}
	}
	width := (highest - lowest) / bins
	if width == 0 {
		width = 1
	}
	histogram := make([]HistogramBin, 0, bins)
	for index := range bins {
		lower := lowest + int64(index)*width
		upper := lower + width
		if index == bins-1 {
			upper = highest
		}
		var count uint64
		for _, point := range points {
			// The final bin is closed so the maximum value is never dropped.
			if point.Value >= lower && (point.Value < upper || (index == bins-1 && point.Value == upper)) {
				count++
			}
		}
		histogram = append(histogram, HistogramBin{LowerBound: lower, UpperBound: upper, Count: count})
	}
	return histogram
}

// forecastSeries fits a least-squares line over the points. It refuses to
// extrapolate from a series whose buckets were not fully observed, because
// projecting from an incomplete record produces a confident wrong answer.
func forecastSeries(points []SeriesPoint, allowIncomplete bool) *ForecastResult {
	forecast := &ForecastResult{BucketsUsed: uint64(len(points))}
	for _, point := range points {
		if point.Coverage != schema.CoverageComplete.String() {
			forecast.IncompleteInput = true
			break
		}
	}
	if forecast.IncompleteInput && !allowIncomplete {
		forecast.RefusedReason = "series contains partial or reconstructed buckets; pass --allow-incomplete to project anyway"
		return forecast
	}
	if len(points) < 2 {
		forecast.RefusedReason = "at least two buckets are required to project a trend"
		return forecast
	}
	var sumX, sumY, sumXY, sumXX float64
	for index, point := range points {
		x, y := float64(index), float64(point.Value)
		sumX += x
		sumY += y
		sumXY += x * y
		sumXX += x * x
	}
	count := float64(len(points))
	denominator := count*sumXX - sumX*sumX
	if denominator == 0 {
		forecast.RefusedReason = "series has no variance to project"
		return forecast
	}
	slope := (count*sumXY - sumX*sumY) / denominator
	intercept := (sumY - slope*sumX) / count
	forecast.PerBucket = slope
	forecast.NextBucketValue = slope*count + intercept
	forecast.ProjectionAmount = int64(forecast.NextBucketValue)
	return forecast
}
