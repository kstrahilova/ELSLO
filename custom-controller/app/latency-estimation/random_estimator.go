package latency_estimation

import (
	"math"
	"math/rand"

	"thesis/types"

	traefikv3 "github.com/traefik/traefik/v3/pkg/provider/kubernetes/crd/traefikio/v1alpha1"
)

type RandomEstimator struct{}

func (estimator *RandomEstimator) Estimate(weightedReplica *traefikv3.LoadBalancerSpec) (*traefikv3.LoadBalancerSpec, error) {
	return weightedReplica, nil
}

func (estimator *RandomEstimator) EstimateAll(weightedReplicas *types.WeightedReplicasList) (*types.WeightedReplicasList, error) {
	return GetRandomWeights(weightedReplicas)
}

func GetRandomWeights(weightedReplicas *types.WeightedReplicasList) (*types.WeightedReplicasList, error) {
	var left float64 = 100.0
	var processedReplicas []traefikv3.LoadBalancerSpec

	for i, weightedReplica := range weightedReplicas.Items {
		var weight int
		if i == len(weightedReplicas.Items)-1 {
			weight = int(left)
		} else {
			weight = int(math.Floor(rand.Float64() * (left / 2)))
		}

		var newService traefikv3.LoadBalancerSpec = traefikv3.LoadBalancerSpec{
			Kind:      weightedReplica.Kind,
			Name:      weightedReplica.Name,
			Namespace: weightedReplica.Namespace,
			Port:      weightedReplica.Port,
			Weight:    &weight,
		}
		processedReplicas = append(processedReplicas, newService)

		left -= float64(weight)
	}

	weightedReplicas.Items = processedReplicas
	return weightedReplicas, nil
}
