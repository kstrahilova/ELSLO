package latency_estimation

import (
	"context"
	"math"
	"sort"
	"sync"

	"thesis/types"

	"github.com/go-logr/logr"
	traefikv3 "github.com/traefik/traefik/v3/pkg/provider/kubernetes/crd/traefikio/v1alpha1"
)

var (
	once sync.Once
	log  logr.Logger
	ctx  context.Context
)

type Estimator interface {
	Estimate(weightedReplica *traefikv3.LoadBalancerSpec) (*traefikv3.LoadBalancerSpec, error)
	EstimateAll(weightedReplicas *types.WeightedReplicasList) (*types.WeightedReplicasList, error)
}

func GetMetricSum(metrics types.ReplicaMetricsList) map[types.MetricType]float64 {

	var sums = map[types.MetricType]float64{}

	for _, values := range metrics {
		for metric, value := range values {
			_, ok := sums[metric]
			if ok {
				sums[metric] += value.Value
			} else {
				sums[metric] = value.Value
			}
		}
	}

	return sums
}

func NormalizeTo100(values map[types.Key]float64) map[types.Key]int {
	type remainder struct {
		key   types.Key
		value float64
	}

	var total float64
	for _, v := range values {
		total += v
	}

	result := make(map[types.Key]int)
	remainders := make([]remainder, 0, len(values))

	sum := 0

	for key, v := range values {
		normalized := v / total * 100
		floor := int(math.Floor(normalized))

		result[key] = floor
		sum += floor

		remainders = append(remainders, remainder{
			key:   key,
			value: normalized - float64(floor),
		})
	}

	sort.SliceStable(remainders, func(i, j int) bool {
		return remainders[i].value > remainders[j].value
	})

	for i := 0; i < 100-sum; i++ {
		result[remainders[i].key]++
	}

	return result
}
