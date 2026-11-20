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
	ewma     *EWMA
	ewmaOnce sync.Once
)

type EWMA struct {
	timeWindow string
	decay      time.Duration
}

func GetEWMA(ctx context.Context, cfg types.Config) (*EWMA, error) {
	if cfg.AggregationFunction != types.Exponential && cfg.AggregationFunction != types.Polynomial {
		return nil, fmt.Errorf("The value %s is an invalid aggregation function", cfg.AggregationFunction.ToString())
	}

	ewmaOnce.Do(func() {
		// Get logger
		log = ctrl.LoggerFrom(emptyCtx)

		d, err := time.ParseDuration(cfg.EwmaDecayParam)
		if err != nil {
			panic(fmt.Errorf("failed to parse duration %s: %w", cfg.EwmaDecayParam, err))
		}

		ewma = &EWMA{
			timeWindow: cfg.AggregationTimeWindow,
			decay:      d,
		}
	})

	return ewma, nil
}

func (ewma *EWMA) ComputeNewAverage(vi float64, ti time.Time, metrics *types.MetricsValues) error {
	oldValue := metrics.Value
	secondsElapsedSinceLastSample := ti.Sub(metrics.LastUpdate).Seconds()
	decay := math.Exp(-secondsElapsedSinceLastSample / ewma.decay.Seconds())
	metrics.LastUpdate = ti
	metrics.Value = math.Max(vi*(1.0-decay)+oldValue*decay, 0.0)

	return nil
}
