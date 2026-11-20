package aggregationalgorithm

import (
	"context"
	"fmt"
	"math"
	"sync"
	"thesis/types"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	decay   *ForwardDecay
	mamOnce sync.Once
)

type ForwardDecay struct {
	timeWindow       string
	function         types.ForwardDecayFunction
	expParam         float64
	polyParam        float64
	landmark         time.Time
	previousLandmark time.Time
	refreshPeriod    time.Duration
}

func GetForwardDecay(ctx context.Context, cfg types.Config) (*ForwardDecay, error) {
	if config.AggregationFunction != types.Exponential && config.AggregationFunction != types.Polynomial {
		return nil, fmt.Errorf("The value %s is an invalid aggregation function", config.AggregationFunction.ToString())
	}

	mamOnce.Do(func() {
		// Get logger
		log = ctrl.LoggerFrom(emptyCtx)
		config = cfg

		decay = &ForwardDecay{
			timeWindow:       config.AggregationTimeWindow,
			function:         config.AggregationFunction,
			expParam:         config.ExpDecayParam,
			polyParam:        config.PolyDecayParam,
			landmark:         time.Now(),
			previousLandmark: time.Now(),
			refreshPeriod:    time.Duration(cfg.ForwardDecayRefreshPeriod) * time.Minute,
		}
	})

	return decay, nil
}

func (forD *ForwardDecay) GetLandmark() time.Time {
	return forD.landmark
}

func (forD *ForwardDecay) GetPreviousLandmark() time.Time {
	return forD.previousLandmark
}

func (forD *ForwardDecay) GetRefreshPeriod() time.Duration {
	return forD.refreshPeriod
}

// Note: we are computing the count and sum separately,
// as we are using the formulas from sections III A and B,
// so we need the positive value of the functionParam!!!
func (forD *ForwardDecay) g(n float64, negParam bool) (float64, error) {
	sign := 1.0
	if negParam {
		sign = -1.0
	}
	switch forD.function {
	case (types.Exponential):
		return math.Exp(n * (sign * forD.expParam)), nil
	case (types.Polynomial):
		return math.Pow((n + 1), (sign * forD.polyParam)), nil
	default:
		return -1.0, fmt.Errorf("The value %s is an invalid aggregation function", forD.function.ToString())
	}
}

func (forD *ForwardDecay) computeNewCount(ti time.Time, t time.Time, numerator float64) AggregateFuncReturnType {
	a, err := forD.g(float64(ti.Sub(forD.landmark).Seconds()), false)
	if err != nil {
		return AggregateFuncReturnType{-1.0, -1.0, err}
	}

	numerator = numerator + a
	b, err := forD.g(float64(t.Sub(forD.landmark).Seconds()), false)
	if err != nil {
		return AggregateFuncReturnType{-1.0, -1.0, err}
	}

	return AggregateFuncReturnType{numerator: numerator, newValue: numerator / b, err: nil}
}

func (forD *ForwardDecay) computeNewSum(vi float64, ti time.Time, t time.Time, numerator float64) AggregateFuncReturnType {

	a, err := forD.g(float64(ti.Sub(forD.landmark).Seconds()), false)
	if err != nil {
		return AggregateFuncReturnType{-1.0, -1.0, err}
	}

	numerator = numerator + a*vi
	b, err := forD.g(float64(t.Sub(forD.landmark).Seconds()), false)
	if err != nil {
		return AggregateFuncReturnType{-1.0, -1.0, err}
	}

	return AggregateFuncReturnType{numerator: numerator, newValue: numerator / b, err: nil}
}

func (forD *ForwardDecay) ComputeNewAverage(vi float64, ti time.Time, metrics *types.MetricsValues) error {
	t := time.Now()

	sum := forD.computeNewSum(vi, ti, t, metrics.SumNumerator)
	if sum.err != nil {
		return sum.err
	}
	log.Info("ComputeNewAverage", "vi", vi, "value", metrics.Value, "sumNum", metrics.SumNumerator, "countNum", metrics.CountNumerator)

	metrics.SumNumerator = sum.numerator

	count := forD.computeNewCount(ti, t, metrics.CountNumerator)
	if count.err != nil {
		return count.err
	}

	metrics.CountNumerator = count.numerator

	metrics.Value = sum.newValue / count.newValue
	metrics.LastUpdate = time.Now()
	return nil
}

func (forD *ForwardDecay) RefreshLandmark() {
	forD.previousLandmark = forD.landmark
	forD.landmark = time.Now()
}

// Given L - original (previous) landmark, and L' - new landmark,
func (forD *ForwardDecay) RescaleRelativeToLandmark(metrics *types.MetricsValues) error {
	diff := forD.landmark.Sub(forD.previousLandmark).Seconds()
	fl := float64(diff)
	factor, err := forD.g(fl, true)
	log.Info("RescaleRelativeToLandmark", "previous", forD.previousLandmark, "current", forD.landmark, "diff", diff, "fl", fl, "factor", factor)
	if err != nil {
		return err
	}

	metrics.Value = metrics.Value * factor
	metrics.SumNumerator = metrics.SumNumerator * factor
	metrics.CountNumerator = metrics.CountNumerator * factor
	return nil
}
