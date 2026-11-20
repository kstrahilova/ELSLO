package latency_estimation

import (
	"context"
	"fmt"
	"math"
	"sync"

	"thesis/metrics"
	"thesis/types"

	aggregation "thesis/aggregation-algo"

	traefikv3 "github.com/traefik/traefik/v3/pkg/provider/kubernetes/crd/traefikio/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	l3Estimator *L3Estimator
	l3Once      sync.Once
)

type L3Estimator struct {
	mu                  sync.Mutex
	metrics             metrics.MetricsApiMetrics
	prometheusMetrics   metrics.PrometheusMetrics
	uptimeMetrics       metrics.ReplicaUptimeMetrics
	reachabilityMetrics metrics.ReachabilityMetrics
	config              types.Config
	algo                aggregation.AggregationAlgorithm
	lastSum             float64
}

func GetL3Estimator(inputCtx context.Context, config types.Config) (*L3Estimator, error) {
	var (
		metr      *metrics.MetricsApiMetrics
		promMetr  *metrics.PrometheusMetrics
		reachMetr *metrics.ReachabilityMetrics
		err       error
	)

	promMetr, err = metrics.GetPrometheusMetrics(inputCtx, config)

	if err != nil {
		return nil, err
	}

	reachMetr, err = metrics.GetReachabilityMetrics(inputCtx, config)

	if err != nil {
		return nil, err
	}

	metr, err = metrics.GetMetricsApiMetrics(inputCtx, config)

	if err != nil {
		return nil, err
	}

	var algo aggregation.AggregationAlgorithm
	if types.ForwardDecay == config.AggregationAlgorithm {
		algo, err = aggregation.GetForwardDecay(ctx, config)
	} else if types.EWMA == config.AggregationAlgorithm {
		algo, err = aggregation.GetEWMA(ctx, config)
	} else {
		return nil, fmt.Errorf("Wrong aggregation algo value")
	}

	if err != nil {
		return nil, err
	}

	l3Once.Do(func() {
		// Get logger
		log = ctrl.LoggerFrom(ctx)
		l3Estimator = &L3Estimator{
			metrics:             *metr,
			prometheusMetrics:   *promMetr,
			uptimeMetrics:       metrics.GetUptimeMetrics(),
			reachabilityMetrics: *reachMetr,
			config:              config,
			algo:                algo,
			lastSum:             100.0,
		}
		ctx = inputCtx
	})

	if l3Estimator.algo == nil {
		return nil, fmt.Errorf("Wrong aggregation algo value")
	}

	return l3Estimator, nil
}

func (estimator *L3Estimator) Estimate(weightedReplica *traefikv3.LoadBalancerSpec) (*traefikv3.LoadBalancerSpec, error) {
	return weightedReplica, nil
}

func (estimator *L3Estimator) EstimateAll(weightedReplicas *types.WeightedReplicasList) (*types.WeightedReplicasList, error) {
	log.Info("Estimating with the L3 Estimator")
	return estimator.getLatencyBasedWeights(weightedReplicas)
}

func (estimator *L3Estimator) getLatencyBasedWeights(weightedReplicas *types.WeightedReplicasList) (*types.WeightedReplicasList, error) {
	var left float64 = 100.0
	var processedReplicas []traefikv3.LoadBalancerSpec

	err := estimator.reachabilityMetrics.GetMetrics(ctx, weightedReplicas)

	if err != nil {
		return nil, err
	}

	metricsMap, absoluteMetricsMap, err := estimator.metrics.GetMetrics(ctx, weightedReplicas)

	if err != nil {
		return nil, err
	}

	_, _, prometheusMetricsMap, successPrometheusMetricsMap, _, err := estimator.prometheusMetrics.GetMetrics(ctx, weightedReplicas)
	if err != nil {
		return nil, err
	}

	absWeights := make(map[types.Key]float64)
	sum := 0.0
	for _, weightedReplica := range weightedReplicas.Items {
		key := types.Key{Name: weightedReplica.Name, Namespace: weightedReplica.Namespace}

		log.Info(key.Name, "R", estimator.uptimeMetrics.GetUptimeR(key.Name, key.Namespace), "M", estimator.uptimeMetrics.GetUptimeM(key.Name, key.Namespace), "P", estimator.uptimeMetrics.GetUptimeP(key.Name, key.Namespace))

		// if not accessible at all
		if !estimator.uptimeMetrics[key].R {
			absWeights[key] = 0
			continue
		}

		var absWeight float64 = 0.0
		prometheusMetrics := prometheusMetricsMap[key]
		successPrometheusMetrics := successPrometheusMetricsMap[key]

		rps := successPrometheusMetrics[types.SuccessCount].Value + successPrometheusMetrics[types.FailureCount].Value
		log.Info(weightedReplica.Name, "rps", rps)
		if rps < 1 {
			rps = 1
		}
		successRate := successPrometheusMetrics[types.SuccessCount].Value / rps
		log.Info(weightedReplica.Name, "successRate", successRate)

		failureLatencySeconds := estimator.config.L3FailurePenalty

		successLatencyP99 := successPrometheusMetrics[types.LatencyMsP99].Value
		log.Info(weightedReplica.Name, "successLatencyP99", successLatencyP99)

		var inFlightRatio = 0.0
		if rps != 0.0 {
			inFlightRatio = prometheusMetrics[types.InFlightRequestCount].Value / rps
		}
		log.Info(weightedReplica.Name, "inFlightRatio", inFlightRatio)

		var estimatedLatency float64
		if successRate == 1.0 {
			estimatedLatency = successLatencyP99
		} else {
			estimatedLatency = successLatencyP99 + failureLatencySeconds*(1.0/successRate-1.0)
		}
		log.Info(weightedReplica.Name, "estimatedLatency", estimatedLatency)

		absWeight = 100.0 / (math.Pow(inFlightRatio+1, 2) * estimatedLatency)
		log.Info(weightedReplica.Name, "absWeight before if statement", absWeight)
		if absWeight < 0.0 {
			absWeight = 1.0
		}

		sum += absWeight
		absWeights[key] = absWeight
		log.Info(weightedReplica.Name, "absWeight after if statement", absWeight)
	}

	log.Info("all", "previous sum", estimator.lastSum, "current sum", sum)
	estimator.lastSum = sum

	c := estimator.GetRelativeChange(absoluteMetricsMap, metricsMap, weightedReplicas)
	log.Info("all", "relative change", c)
	avgWeight := GetAverageWeight(absWeights)
	log.Info("all", "average weight", avgWeight)
	newWeights := absWeights

	log.Info("Before normalizing weights")
	normalizedWeights := NormalizeTo100(newWeights)

	if estimator.config.L3RateControllerEnabled {
		log.Info("Before rate control", "c", c)
		log.Info("Before rate control", "avgWeight", avgWeight)
		log.Info("Before rate control", "absWeights", absWeights)
		log.Info("Before rate control", "normalizedWeights", normalizedWeights)
		newWeights = estimator.RateControl(c, avgWeight, weightedReplicas, normalizedWeights, ctx)
	}

	for _, weightedReplica := range weightedReplicas.Items {
		key := types.Key{Name: weightedReplica.Name, Namespace: weightedReplica.Namespace}
		ratio := (absWeights[key] / sum) * 100
		newRatio := (newWeights[key] / sum) * 100
		var weight = normalizedWeights[key]

		log.Info(weightedReplica.Name, "expLatency", 1/absWeights[key], "1/expLatency", absWeights[key], "sum", sum, "ratio", ratio, "newRatio", newRatio, "weight", weight)
		log.Info(weightedReplica.Name, "weight", weight)

		nativeLB := true
		var newService traefikv3.LoadBalancerSpec = traefikv3.LoadBalancerSpec{
			Kind:      weightedReplica.Kind,
			Name:      weightedReplica.Name,
			Namespace: weightedReplica.Namespace,
			Port:      weightedReplica.Port,
			Weight:    &weight,
			NativeLB:  &nativeLB,
		}
		processedReplicas = append(processedReplicas, newService)

		left -= float64(weight)
	}

	weightedReplicas.Items = processedReplicas

	return weightedReplicas, nil
}

func (estimator *L3Estimator) RateControl(c float64, avgWeight float64, weightedReplicas *types.WeightedReplicasList, currentWeights map[types.Key]int, ctx context.Context) map[types.Key]float64 {
	var newWeights map[types.Key]float64 = make(map[types.Key]float64)

	for _, weightedReplica := range weightedReplicas.Items {
		log.Info("Processing replica", "name", weightedReplica.Name)
		key := types.Key{Name: weightedReplica.Name, Namespace: weightedReplica.Namespace}
		currentWeight := float64(*weightedReplica.Weight)
		log.Info("Processing replica", "name", weightedReplica.Name, "currentWeight", currentWeight)
		if c > 0 {
			log.Info(weightedReplica.Name, "c", c)
			denom := math.Pow(1+math.Pow(c, 2), 3/2)
			log.Info(weightedReplica.Name, "denom", denom)
			newWeight := avgWeight - avgWeight/denom + currentWeight/denom
			newWeights[key] = newWeight
			log.Info("New weight calculated", "name", weightedReplica.Name, "newWeight", newWeight)
		} else {
			log.Info(weightedReplica.Name, "c <= 0", c)
			if currentWeight <= avgWeight {
				log.Info(weightedReplica.Name, "case", "currentWeight <= avgWeight", "currentWeight", currentWeight, "avgWeight", avgWeight)
				denom := math.Pow(1+2*math.Pow(c, 2), 3/2)
				log.Info(weightedReplica.Name, "denom", denom)
				newWeight := currentWeight / denom
				newWeights[key] = newWeight
				log.Info("New weight calculated", "name", weightedReplica.Name, "newWeight", newWeight)
			} else {
				log.Info(weightedReplica.Name, "case", "currentWeight > avgWeight", "currentWeight", currentWeight, "avgWeight", avgWeight)
				denom := math.Pow(1+3*math.Pow(c, 2), 3/2)
				log.Info(weightedReplica.Name, "denom", denom)
				newWeight := 2*currentWeight - avgWeight - (currentWeight-avgWeight)/denom
				newWeights[key] = newWeight
				log.Info("New weight calculated", "name", weightedReplica.Name, "newWeight", newWeight)
			}
		}

		if newWeights[key] < 1 {
			log.Info("Weight below 1, setting to 1", "name", weightedReplica.Name, "newWeight", newWeights[key])
			newWeights[key] = 1
		}
	}
	log.Info("Returning new weights after rate control")
	return newWeights
}

func GetAverageWeight(weights map[types.Key]float64) float64 {
	if len(weights) == 0 {
		return 0
	}

	var sum float64 = 0
	for _, weight := range weights {
		sum += weight
	}

	return sum / float64(len(weights))
}

func (estimator *L3Estimator) GetRelativeChange(absoluteMetricsMap types.AbsoluteReplicaMetricsList, metricsMap types.ReplicaMetricsList, weightedReplicas *types.WeightedReplicasList) float64 {
	// rps
	sumAggr := 0.0
	// totalRequestsPerSecond
	sumAbs := 0.0

	for _, item := range weightedReplicas.Items {
		key := types.Key{Name: item.Name, Namespace: item.Namespace}
		sumAggr += metricsMap[key][types.SuccessCount].Value + metricsMap[key][types.FailureCount].Value
		sumAbs += absoluteMetricsMap[key][types.SuccessCount] + absoluteMetricsMap[key][types.FailureCount]
	}

	return (sumAbs - sumAggr) / sumAggr
}

func (estimator *L3Estimator) GetRelativeChangeMyInterpretaton(absoluteMetricsMap types.AbsoluteReplicaMetricsList, metricsMap types.ReplicaMetricsList, weightedReplicas *types.WeightedReplicasList) float64 {
	sumAggr := 0.0
	sumAbs := 0.0

	for _, item := range weightedReplicas.Items {
		key := types.Key{Name: item.Name, Namespace: item.Namespace}
		sumAggr += metricsMap[key][types.SuccessCount].Value + metricsMap[key][types.FailureCount].Value
		sumAbs += absoluteMetricsMap[key][types.SuccessCount] + absoluteMetricsMap[key][types.FailureCount]
	}

	return sumAbs - sumAggr
}
