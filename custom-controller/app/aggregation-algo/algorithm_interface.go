package aggregationalgorithm

import (
	"context"
	"sync"
	"thesis/types"
	"time"

	"github.com/go-logr/logr"
)

var (
	mu       sync.Mutex
	log      logr.Logger
	config   types.Config
	emptyCtx context.Context
)

type AggregationAlgorithm interface {
	ComputeNewAverage(vi float64, ti time.Time, metrics *types.MetricsValues) error
}

type State struct {
}

type AggregateFuncReturnType struct {
	numerator float64
	newValue  float64
	err       error
}
