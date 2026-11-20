package types

import (
	"encoding/json"
	"fmt"
)

type Algorithm int

const (
	ForwardDecay Algorithm = iota
	EWMA
	UnknownAlgorithm
)

type EstimationAlgorithm int

const (
	ELSLO EstimationAlgorithm = iota
	L3
	ELS
	UnknownEstimationAlgorithm
)

func MapAlgorithm(input string) (Algorithm, error) {
	switch input {
	case "decay":
		return ForwardDecay, nil
	case "ewma":
		return EWMA, nil
	default:
		return UnknownAlgorithm, fmt.Errorf("The value %s set for aggregation algorithm is not recognised. Allowed options are \"exp\" for Exponential Forward decay, \"poly\" for Polynomial Forward decay, and \"ewma\" for EWMA", input)
	}
}
func MapLatencyEstimationAlgorithm(input string) (EstimationAlgorithm, error) {
	switch input {
	case "latency":
		return ELSLO, nil
	case "l3":
		return L3, nil
	case "els":
		return ELS, nil
	default:
		return UnknownEstimationAlgorithm, fmt.Errorf("The value %s set for latency estimation algorithm is not recognised. Allowed options are \"latency\"", input)
	}
}

type ForwardDecayFunction int

const (
	Polynomial ForwardDecayFunction = iota
	Exponential
	UnknownFunction
)

func (f ForwardDecayFunction) ToString() string {
	switch f {
	case Polynomial:
		return "poly"
	case Exponential:
		return "exp"
	default:
		return "unknown"
	}
}

func MapForwardDecayFunction(input string) (ForwardDecayFunction, error) {
	switch input {
	case "poly":
		return Polynomial, nil
	case "exp":
		return Exponential, nil
	default:
		return UnknownFunction, fmt.Errorf("The value %s set for forward decay function is not recognised. Allowed options are \"exp\" for Exponential Forward decay, \"poly\" for Polynomial Forward decay, and \"ewma\" for EWMA", input)
	}
}

func MapSloMetricThresholds(input string) (map[SloViolationMetric]int, error) {
	tmp := make(map[string]int)
	if err := json.Unmarshal([]byte(input), &tmp); err != nil {
		return nil, err
	}

	sloThresholds := make(map[SloViolationMetric]int)
	for k, v := range tmp {
		metr, err := MapThresholdMetricToSloViolationMetric(k)
		if err != nil {
			return nil, err
		}
		sloThresholds[metr] = v
	}
	return sloThresholds, nil
}

type Config struct {
	MetricsAddress            string
	PrometheusAddress         string
	MetricsTimeWindow         string
	PrometheusTimeWindow      string
	EstimationAlgorithm       EstimationAlgorithm
	AggregationAlgorithm      Algorithm
	AggregationFunction       ForwardDecayFunction
	ExpDecayParam             float64
	PolyDecayParam            float64
	EwmaDecayParam            string
	AggregationTimeWindow     string
	ForwardDecayRefreshPeriod float64
	SloMetricThresholds       map[SloViolationMetric]int
	FailurePenalty            float64
	L3FailurePenalty          float64
	L3RateControllerEnabled   bool
}
