package types

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"thesis/metrics-api-client/linkerd2/viz/metrics-api/gen/viz"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
)

var log logr.Logger = ctrl.LoggerFrom(emptyCtx)

type MetricsValues struct {
	Value          float64
	SumNumerator   float64
	CountNumerator float64
	LastUpdate     time.Time
}

type MetricType int

const (
	SuccessCount MetricType = iota
	FailureCount
	LatencyMsP50
	LatencyMsP95
	LatencyMsP99
	ActualSuccessCount
	ActualFailureCount
	InFlightRequestCount
	UnknownMetricType
)

func (t MetricType) ToString() string {
	switch t {
	case SuccessCount:
		return "success_count"
	case FailureCount:
		return "failure_count"
	case LatencyMsP50:
		return "latency_ms_p50"
	case LatencyMsP95:
		return "latency_ms_p95"
	case LatencyMsP99:
		return "latency_ms_p99"
	case ActualSuccessCount:
		return "actual_success_count"
	case ActualFailureCount:
		return "actual_failure_count"
	case InFlightRequestCount:
		return "in_flight_request_count"
	default:
		return "unknown"
	}
}

func MapMetricType(input string) (MetricType, error) {
	switch input {
	case "success_count":
		return SuccessCount, nil
	case "failure_count":
		return FailureCount, nil
	case "latency_ms_p50":
		return LatencyMsP50, nil
	case "latency_ms_p95":
		return LatencyMsP95, nil
	case "latency_ms_p99":
		return LatencyMsP99, nil
	case "actual_success_count":
		return ActualSuccessCount, nil
	case "actual_failure_count":
		return ActualFailureCount, nil
	case "in_flight_request_count":
		return InFlightRequestCount, nil
	default:
		return UnknownMetricType, fmt.Errorf("The value %s set for aggregation algorithm is not recognised. Allowed options are \"exp\" for Exponential Forward decay, \"poly\" for Polynomial Forward decay, and \"ewma\" for EWMA", input)
	}
}

var initMetricApiStats = map[MetricType]MetricsValues{
	SuccessCount:       {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
	FailureCount:       {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
	LatencyMsP50:       {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
	LatencyMsP95:       {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
	LatencyMsP99:       {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
	ActualSuccessCount: {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
	ActualFailureCount: {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
}

var initAbsoluteMetricApiStats = map[MetricType]float64{
	SuccessCount:       1,
	FailureCount:       1,
	LatencyMsP50:       1,
	LatencyMsP95:       1,
	LatencyMsP99:       1,
	ActualSuccessCount: 1,
	ActualFailureCount: 1,
}

var initPrometheusStats = map[MetricType]MetricsValues{
	SuccessCount:         {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
	FailureCount:         {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
	LatencyMsP50:         {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
	LatencyMsP95:         {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
	LatencyMsP99:         {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
	InFlightRequestCount: {Value: 1, SumNumerator: 1, CountNumerator: 1, LastUpdate: time.Now()},
}

var initAbsolutePrometheusStats = map[MetricType]float64{
	SuccessCount:         1,
	FailureCount:         1,
	LatencyMsP50:         1,
	LatencyMsP95:         1,
	LatencyMsP99:         1,
	InFlightRequestCount: 1,
}

type SloViolationMetric int

const (
	Latency SloViolationMetric = iota
	UnknownSloMetricType
)

func (t SloViolationMetric) ToQueryMetric() string {
	switch t {
	case Latency:
		return "response_latency_ms"
	default:
		return "unknown"
	}
}

func (t SloViolationMetric) ToMetricType(p int) MetricType {
	switch t {
	case Latency:
		switch p {
		case 50:
			return LatencyMsP50
		case 95:
			return LatencyMsP95
		case 99:
			return LatencyMsP99
		default:
			return UnknownMetricType
		}
	default:
		return UnknownMetricType
	}
}

func MapQueryMetricToSloViolationMetric(input string) (SloViolationMetric, error) {
	switch input {
	case "response_latency_ms":
		return Latency, nil
	default:
		return UnknownSloMetricType, fmt.Errorf("The value %s set for aggregation algorithm is not recognised. Allowed options are \"exp\" for Exponential Forward decay, \"poly\" for Polynomial Forward decay, and \"ewma\" for EWMA", input)
	}
}

func MapThresholdMetricToSloViolationMetric(input string) (SloViolationMetric, error) {
	switch input {
	case "latency":
		return Latency, nil
	default:
		return UnknownSloMetricType, fmt.Errorf("The value %s set for aggregation algorithm is not recognised. Allowed options are \"exp\" for Exponential Forward decay, \"poly\" for Polynomial Forward decay, and \"ewma\" for EWMA", input)
	}
}

var initSloViolationMetrics = map[SloViolationMetric]MetricsValues{
	Latency: {Value: 0, SumNumerator: 0, CountNumerator: 0},
}

var emptyCtx context.Context

type Key struct {
	Name      string
	Namespace string
}

type ReplicaMetricsList map[Key]map[MetricType]*MetricsValues

type AbsoluteReplicaMetricsList map[Key]map[MetricType]float64

type ReplicaSloViolationsList map[Key]map[SloViolationMetric]*MetricsValues

func (rml ReplicaMetricsList) getPrintable() map[string]map[string]*MetricsValues {
	printable := make(map[string]map[string]*MetricsValues)
	for k, v := range rml {
		strKey := fmt.Sprintf("[%s,%s]", k.Name, k.Namespace)

		inner := make(map[string]*MetricsValues)
		for k2, v2 := range v {
			inner[k2.ToString()] = v2
		}

		printable[strKey] = inner
	}

	return printable
}

func (replicaMetricsList ReplicaMetricsList) GetReplicaMetricsByNameAndNamespace(name string, namespace string, log logr.Logger) (map[MetricType]*MetricsValues, error) {
	stats, ok := replicaMetricsList[Key{Name: name, Namespace: namespace}]

	if ok {
		return stats, nil
	}

	return nil, fmt.Errorf("the replica metrics for replica %s in the %s namespace is not in the list of replica weightss, so it could not be returned", name, namespace)
}

func (replicaMetricsList AbsoluteReplicaMetricsList) GetReplicaMetricsByNameAndNamespace(name string, namespace string, log logr.Logger) (map[MetricType]float64, error) {
	stats, ok := replicaMetricsList[Key{Name: name, Namespace: namespace}]

	if ok {
		return stats, nil
	}

	return nil, fmt.Errorf("the absolute replica metrics for replica %s in the %s namespace is not in the list of replica weights, so it could not be returned", name, namespace)
}

func (replicaMetricsList ReplicaMetricsList) SetReplicaMetrics(name string, namespace string, stats map[MetricType]*MetricsValues) {
	replicaMetricsList[Key{Name: name, Namespace: namespace}] = stats
}

func (replicaMetricsList ReplicaMetricsList) ReplicaMetricsExists(name string, namespace string) bool {
	_, ok := replicaMetricsList[Key{Name: name, Namespace: namespace}]

	return ok
}

func (absoluteReplicaMetricsList AbsoluteReplicaMetricsList) ReplicaMetricsExists(name string, namespace string) bool {
	_, ok := absoluteReplicaMetricsList[Key{Name: name, Namespace: namespace}]

	return ok
}

func (replicaMetricsList ReplicaMetricsList) InitReplicaMetrics(name string, namespace string, prom bool) {
	ctrl.LoggerFrom(emptyCtx).Info("Initializing replica metrics for", "name", name)
	if replicaMetricsList.ReplicaMetricsExists(name, namespace) {
		return
	}

	stats := map[MetricType]*MetricsValues{}

	var initStats map[MetricType]MetricsValues
	if prom {
		initStats = initPrometheusStats
	} else {
		initStats = initMetricApiStats
	}

	for k, v := range initStats {
		pv := v
		stats[k] = &pv
	}

	replicaMetricsList[Key{Name: name, Namespace: namespace}] = stats
}
func (replicaMetricsList AbsoluteReplicaMetricsList) InitReplicaMetrics(name string, namespace string, prom bool) {

	ctrl.LoggerFrom(emptyCtx).Info("Initializing replica absolute metrics for", "name", name)
	if replicaMetricsList.ReplicaMetricsExists(name, namespace) {
		return
	}

	stats := map[MetricType]float64{}

	var initStats map[MetricType]float64
	if prom {
		initStats = initAbsolutePrometheusStats
	} else {
		initStats = initAbsoluteMetricApiStats
	}

	maps.Copy(stats, initStats)

	replicaMetricsList[Key{Name: name, Namespace: namespace}] = stats
}

func MapVizBasicStatsToMeasuredMetrics(stats *viz.BasicStats) (map[MetricType]float64, error) {
	data, _ := json.Marshal(stats)
	var intermediateResult map[string]float64
	json.Unmarshal(data, &intermediateResult)

	result := make(map[MetricType]float64)
	for k, v := range intermediateResult {
		key, err := MapMetricType(k)
		if err != nil {
			return nil, err
		}
		result[key] = v
	}
	return result, nil
}

func (rsvl ReplicaSloViolationsList) GetReplicaSloViolationsByNameAndNamespace(name string, namespace string, log logr.Logger) (map[SloViolationMetric]*MetricsValues, error) {
	stats, ok := rsvl[Key{Name: name, Namespace: namespace}]

	if ok {
		return stats, nil
	}

	return nil, fmt.Errorf("the replica SLO violations for replica %s in the %s namespace is not in the list of replica weights, so it could not be returned", name, namespace)
}

func (rsvl ReplicaSloViolationsList) SetReplicaSloViolations(name string, namespace string, sloViolations map[SloViolationMetric]*MetricsValues) {
	rsvl[Key{Name: name, Namespace: namespace}] = sloViolations
}

func (rsvl ReplicaSloViolationsList) ReplicaSloViolationsExists(name string, namespace string) bool {
	_, ok := rsvl[Key{Name: name, Namespace: namespace}]

	return ok
}

func (rsvl ReplicaSloViolationsList) InitReplicaSloViolationsMetrics(name string, namespace string) {
	ctrl.LoggerFrom(emptyCtx).Info("Initializing replica SLO violations for", "name", name)
	if rsvl.ReplicaSloViolationsExists(name, namespace) {
		return
	}

	stats := map[SloViolationMetric]*MetricsValues{}

	for k, v := range initSloViolationMetrics {
		pv := v
		stats[k] = &pv
	}

	rsvl[Key{Name: name, Namespace: namespace}] = stats
}
