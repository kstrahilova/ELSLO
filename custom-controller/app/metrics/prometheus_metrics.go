package metrics

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"sync"

	aggregation "thesis/aggregation-algo"
	"thesis/types"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	successfulBucketQuery = "sum by (dst_service, le) (irate(%s_bucket{status_code=~\"(2..|3..)\", deployment=~\"traefik\", dst_service=~\"%s\", le=~\"%s\", direction=\"outbound\", authority!~\".*8081.*\"}[%s]))"
	failedBucketQuery     = "sum by (dst_service, le) (irate(%s_bucket{status_code!~\"(2..|3..)\", deployment=~\"traefik\", dst_service=~\"%s\", direction=\"outbound\", authority!~\".*8081.*\"}[%s]))"
	totalBucketQuery      = "sum by (dst_service, le) (irate(%s_bucket{deployment=~\"traefik\", dst_service=~\"%s\", direction=\"outbound\", authority!~\".*8081.*\"}[%s]))"
	failedCountQuery      = "sum by (dst_service, le) (irate(%s_count{status_code!~\"(2..|3..)\", deployment=~\"traefik\", dst_service=~\"%s\", direction=\"outbound\", authority!~\".*8081.*\"}[%s]))"
	sumQuery              = "sum by (dst_service, le) (irate(%s_sum{status_code=~\"(2..|3..)\",deployment=~\"traefik\", dst_service=~\"%s\", direction=\"outbound\", authority!~\".*8081.*\"}[%s]))"
	failedSumQuery        = "sum by (dst_service, le) (irate(%s_sum{status_code!~\"(2..|3..)\",deployment=~\"traefik\", dst_service=~\"%s\", direction=\"outbound\", authority!~\".*8081.*\"}[%s]))"
	// this is used for the inflight requests, no need to filter out any status codes
	totalQuery = "sum by (dst_service, le) (irate(%s_total{classification=~\"%s\", deployment=~\"traefik\", dst_service=~\"%s\", direction=\"outbound\", authority!~\".*8081.*\"}[%s]))"
)

var (
	prometheusMetrics *PrometheusMetrics
	pmOnce            sync.Once
	emptyBucket       types.PrometheusBucket = types.PrometheusBucket{Le: "+Inf", Count: 0, Timestamp: time.Now().UTC()}
)

const (
	anyValueConst = ".*"
)

type PrometheusMetrics struct {
	prometheusApi                    v1.API
	algo                             aggregation.AggregationAlgorithm
	code200ViolationMetrics          types.ReplicaSloViolationsList
	non200ViolationMetrics           types.ReplicaSloViolationsList
	totalMetrics                     types.ReplicaMetricsList
	successfultMetrics               types.ReplicaMetricsList
	failedMetrics                    types.ReplicaMetricsList
	config                           types.Config
	previousReplicaLatencyHistograms types.ReplicaLatencyHistogramList
}

type QueryResult struct {
	timestamp time.Time
	value     *float64
	buckets   *map[string]types.PrometheusBucket
}

type NoDataReceived struct {
	Code    int
	Message string
	Query   string
}

func (e *NoDataReceived) Error() string {
	return fmt.Sprintf("Code %d: %s for query %s", e.Code, e.Message, e.Query)
}

func GetPrometheusMetrics(ctx context.Context, config types.Config) (*PrometheusMetrics, error) {
	pmOnce.Do(func() {
		log = ctrl.LoggerFrom(emptyCtx)
		prometheusApiClient, err := api.NewClient(api.Config{Address: config.PrometheusAddress})
		if err != nil {
			panic(1)
		}

		var algo aggregation.AggregationAlgorithm
		if types.ForwardDecay == config.AggregationAlgorithm {
			algo, err = aggregation.GetForwardDecay(ctx, config)
		} else if types.EWMA == config.AggregationAlgorithm {
			algo, err = aggregation.GetEWMA(ctx, config)
		} else {
			panic("Wrong aggregation algo value")
		}

		if err != nil {
			panic(err)
		}

		prometheusMetrics = &PrometheusMetrics{
			code200ViolationMetrics:          types.ReplicaSloViolationsList{},
			non200ViolationMetrics:           types.ReplicaSloViolationsList{},
			totalMetrics:                     types.ReplicaMetricsList{},
			successfultMetrics:               types.ReplicaMetricsList{},
			failedMetrics:                    types.ReplicaMetricsList{},
			prometheusApi:                    v1.NewAPI(prometheusApiClient),
			config:                           config,
			algo:                             algo,
			previousReplicaLatencyHistograms: types.ReplicaLatencyHistogramList{},
		}
	})

	if prometheusMetrics.algo == nil {
		return nil, fmt.Errorf("Wrong aggregation algo value")
	}

	return prometheusMetrics, nil
}

func (prometheusMetrics *PrometheusMetrics) GetReplicaPrometheusHistogram(name string, namespace string) types.PrometheusHistogram {
	ctrl.LoggerFrom(emptyCtx).Info("Getting replica Prometheus histogram for", "name", name, "namespace", namespace)
	key := types.Key{Name: name, Namespace: namespace}
	if !prometheusMetrics.ReplicaPrometheusHistogramExists(name, namespace) {
		ctrl.LoggerFrom(emptyCtx).Info("Replica Prometheus histogram does not exist, initializing", "name", name, "namespace", namespace)
		prometheusMetrics.previousReplicaLatencyHistograms[key] = types.InitPrometheusHistogram()
	}

	return prometheusMetrics.previousReplicaLatencyHistograms[key]
}

func (prometheusMetrics *PrometheusMetrics) ReplicaPrometheusHistogramExists(name string, namespace string) bool {
	_, ok := prometheusMetrics.previousReplicaLatencyHistograms[types.Key{Name: name, Namespace: namespace}]

	return ok
}

func (prometheusMetrics *PrometheusMetrics) GetMetrics(ctx context.Context, weightedReplicas *types.WeightedReplicasList) (types.ReplicaSloViolationsList, types.ReplicaSloViolationsList, types.ReplicaMetricsList, types.ReplicaMetricsList, types.ReplicaMetricsList, error) {
	uptimeMu.Lock()
	defer uptimeMu.Unlock()

	for _, replica := range weightedReplicas.Items {

		if !prometheusMetrics.code200ViolationMetrics.ReplicaSloViolationsExists(replica.Name, replica.Namespace) {
			log.Info("replicaSloViolations doesn't exist, so we initialize")
			prometheusMetrics.code200ViolationMetrics.InitReplicaSloViolationsMetrics(replica.Name, replica.Namespace)
			log.Info("replicaSloViolations was initialized")
		}

		if !prometheusMetrics.non200ViolationMetrics.ReplicaSloViolationsExists(replica.Name, replica.Namespace) {
			log.Info("non200ReplicaSloViolations doesn't exist, so we initialize")
			prometheusMetrics.non200ViolationMetrics.InitReplicaSloViolationsMetrics(replica.Name, replica.Namespace)
			log.Info("non200ReplicaSloViolations was initialized")
		}

		if !prometheusMetrics.totalMetrics.ReplicaMetricsExists(replica.Name, replica.Namespace) {
			log.Info("metrics doesn't exist, so we initialize")
			prometheusMetrics.totalMetrics.InitReplicaMetrics(replica.Name, replica.Namespace, true)
			log.Info("metrics was initialized")
		}

		if !prometheusMetrics.successfultMetrics.ReplicaMetricsExists(replica.Name, replica.Namespace) {
			log.Info("metrics doesn't exist, so we initialize")
			prometheusMetrics.successfultMetrics.InitReplicaMetrics(replica.Name, replica.Namespace, true)
			log.Info("metrics was initialized")
		}

		if !prometheusMetrics.failedMetrics.ReplicaMetricsExists(replica.Name, replica.Namespace) {
			log.Info("failedMetrics doesn't exist, so we initialize")
			prometheusMetrics.failedMetrics.InitReplicaMetrics(replica.Name, replica.Namespace, true)
			log.Info("failedMetrics was initialized")
		}

		if !uptimeMetrics.UptimeMetricsExists(replica.Name, replica.Namespace) {
			log.Info("UptimeMetrics doesn't exist, so we initialize to false")
			uptimeMetrics.InitUptime(replica.Name, replica.Namespace)
		}

		err := prometheusMetrics.RefreshMetricDataFromPrometheus(replica.Name, replica.Namespace)

		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
	}

	return prometheusMetrics.code200ViolationMetrics, prometheusMetrics.non200ViolationMetrics, prometheusMetrics.totalMetrics, prometheusMetrics.successfultMetrics, prometheusMetrics.failedMetrics, nil
}

func (prometheusMetrics *PrometheusMetrics) RefreshMetricDataFromPrometheus(service string, namespace string) error {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var missingEntry *NoDataReceived

	for metric, threshold := range prometheusMetrics.config.SloMetricThresholds {

		// Get the buckets for the successful latency histogram
		bq := fmt.Sprintf(successfulBucketQuery, metric.ToQueryMetric(), service, anyValueConst, prometheusMetrics.config.PrometheusTimeWindow)
		successfulBucketsResult, err := GetResult(service, bq, timeoutCtx)

		if err != nil {
			log.Error(err, "error", "service", service, "query", bq)
			if errors.As(err, &missingEntry) {
				uptimeMetrics.SetUptimeP(service, namespace, false)
			} else {
				return err
			}
		}

		if successfulBucketsResult.buckets == nil {
			log.Info(service, "buckets result", "nil")
			os.Exit(1)
		}

		// Get the failed buckets for the latency histogram
		fbq := fmt.Sprintf(failedBucketQuery, metric.ToQueryMetric(), service, prometheusMetrics.config.PrometheusTimeWindow)
		failedBucketsResult, err := GetResult(service, fbq, timeoutCtx)

		if err != nil {
			log.Error(err, "error", "service", service, "query", fbq)
			if errors.As(err, &missingEntry) {
				uptimeMetrics.SetUptimeP(service, namespace, false)
			} else {
				return err
			}
		}

		if failedBucketsResult.buckets == nil {
			log.Info(service, "failed buckets result", "nil")
			os.Exit(1)
		}

		failedResponseBucket := (*failedBucketsResult.buckets)["+Inf"]
		failedResponseCount := failedResponseBucket.Count

		failedResponseWithinThresholdBucket := (*failedBucketsResult.buckets)[strconv.Itoa(threshold)]
		failedResponseWithinThresholdCount := failedResponseWithinThresholdBucket.Count

		// Get the number of requests that respect the SLO threshold
		var nonViolationsBucket = types.PrometheusBucket{
			Count:     0,
			Le:        "+Inf",
			Timestamp: time.Now().UTC(),
		}
		if nb, ok := (*successfulBucketsResult.buckets)[strconv.Itoa(threshold)]; ok {
			nonViolationsBucket = nb
		} else {
			log.Info(service, "all requests violate slo, reason:", "no successful requests found within threshold")
		}

		var nonViolations = nonViolationsBucket.Count

		log.Info(service, "successful requests within threshold", nonViolations)

		// Get the number of successful responses for the latency histogram
		var successResponseCount = 0.0
		var successResponseBucket = types.PrometheusBucket{
			Count:     0,
			Le:        "+Inf",
			Timestamp: time.Now().UTC(),
		}

		if successResponseBucket, ok := (*successfulBucketsResult.buckets)["+Inf"]; ok {
			successResponseCount = successResponseBucket.Count
		} else {
			log.Info(service, "all requests violate slo, reason:", "no successful requests found")
		}

		log.Info(service, "total number of successful requests", successResponseCount)

		// Get the count for the latency histogram
		tbq := fmt.Sprintf(totalBucketQuery, metric.ToQueryMetric(), service, prometheusMetrics.config.PrometheusTimeWindow)
		totalBucketResult, err := GetResult(service, tbq, timeoutCtx)

		if err != nil {
			log.Error(err, "error", "service", service, "query", tbq)
			if errors.As(err, &missingEntry) {
				uptimeMetrics.SetUptimeP(service, namespace, false)
				// return nil
			} else {
				return err
			}
		}

		totalResponseCount := 0.0
		totalResponseBucket := types.PrometheusBucket{
			Count:     0,
			Le:        "+Inf",
			Timestamp: time.Now().UTC(),
		}
		if totalResponseBucket, ok := (*totalBucketResult.buckets)["+Inf"]; ok {
			totalResponseCount = totalResponseBucket.Count
		} else {
			log.Info(service, "all requests violate slo, reason:", "no total requests found")
		}
		totalCount := totalResponseCount
		log.Info(service, "total number of any requests", totalCount)

		// Get the number of requests for the latency histogram
		reqQ := fmt.Sprintf(totalQuery, "request", anyValueConst, service, prometheusMetrics.config.PrometheusTimeWindow)
		requestCountResult, err := GetResult(service, reqQ, timeoutCtx)

		if err != nil {
			log.Error(err, "error", "service", service, "query", reqQ)
			if errors.As(err, &missingEntry) {
				uptimeMetrics.SetUptimeP(service, namespace, false)
			} else {
				return err
			}
		}

		requestCount := *(requestCountResult.value)

		// Get the number of responses for the latency histogram
		resQ := fmt.Sprintf(totalQuery, "response", anyValueConst, service, prometheusMetrics.config.PrometheusTimeWindow)
		responseCountResult, err := GetResult(service, resQ, timeoutCtx)

		if err != nil {
			log.Error(err, "error", "service", service, "query", resQ)
			if errors.As(err, &missingEntry) {
				uptimeMetrics.SetUptimeP(service, namespace, false)
			} else {
				return err
			}
		}

		responseCount := *(responseCountResult.value)

		// Setting uptime to true for the service
		uptimeMetrics.SetUptimeP(service, namespace, true)
		successfulReplicaMetrics, err := prometheusMetrics.successfultMetrics.GetReplicaMetricsByNameAndNamespace(service, namespace, log)
		if err != nil {
			return err
		}

		// Include the new successful response count in the replica metrics
		err = prometheusMetrics.algo.ComputeNewAverage(successResponseCount, successResponseBucket.Timestamp, successfulReplicaMetrics[types.SuccessCount])
		if err != nil {
			return err
		}

		// Include the new failed response count in the replica metrics
		err = prometheusMetrics.algo.ComputeNewAverage(failedResponseCount, failedResponseBucket.Timestamp, successfulReplicaMetrics[types.FailureCount])
		if err != nil {
			return err
		}

		var successfulP99 float64
		if successResponseCount > 0 {
			log.Info("Requesting successful p99 latency for service", "service", service)
			successfulP99, err = GetLatency(service, namespace, successfulBucketsResult, successResponseBucket, 0.99, emptyCtx)
			if err != nil {
				log.Info(service, "error", err)
				if successfulP99 == math.Inf(1) {
					log.Info(service, "message", "p99 latency is infinity for service")
				} else {
					return err
				}
			}
		} else if failedResponseCount > 0 {
			log.Info("All requests fail, setting p99 latency to infinity for service", "service", service)
			successfulP99 = math.Inf(1)
		} else {
			log.Info("No successful or failed requests, setting p99 latency to 200 for service", "service", service)
			// TODO: Change this to the SLO threshold
			successfulP99 = 200
		}

		// Include the new p99 latency in the replica metrics
		err = prometheusMetrics.algo.ComputeNewAverage(successfulP99, successfulBucketsResult.timestamp, successfulReplicaMetrics[metric.ToMetricType(99)])
		if err != nil {
			return err
		}

		var failedP99 float64
		if failedResponseCount > 0 {
			log.Info("Requesting failed p99 latency for service", "service", service)
			failedP99, err = GetLatency(service, namespace, failedBucketsResult, failedResponseBucket, 0.99, emptyCtx)
			if err != nil {
				log.Info(service, "error", err)
				if failedP99 == math.Inf(1) {
					log.Info(service, "message", "failed p99 latency is infinity for service")
				} else {
					return err
				}
			}
		} else {
			log.Info("No failed requests, setting failed p99 latency to 0 for service", "service", service)
			failedP99 = 0
		}

		failedReplicaMetrics, err := prometheusMetrics.failedMetrics.GetReplicaMetricsByNameAndNamespace(service, namespace, log)
		if err != nil {
			return err
		}

		var totalP99 float64
		if totalResponseCount > 0 {
			log.Info("Requesting total p99 latency for service", "service", service)
			totalP99, err = GetLatency(service, namespace, totalBucketResult, totalResponseBucket, 0.99, emptyCtx)
			if err != nil {
				log.Info(service, "error", err)
				if totalP99 == math.Inf(1) {
					log.Info(service, "message", "total p99 latency is infinity for service")
				} else {
					return err
				}
			}
		} else {
			log.Info("Setting total p99 latency to infinity for service", "service", service)
			totalP99 = math.Inf(1)
		}

		// Include the new p99 latency in the replica metrics
		err = prometheusMetrics.algo.ComputeNewAverage(totalP99, totalBucketResult.timestamp, successfulReplicaMetrics[metric.ToMetricType(99)])
		if err != nil {
			return err
		}

		// Include the new failed p99 latency in the failed replica metrics
		log.Info(service, "will now compute", "the failure p99", "failedp99", failedP99)
		err = prometheusMetrics.algo.ComputeNewAverage(failedP99, failedBucketsResult.timestamp, failedReplicaMetrics[metric.ToMetricType(99)])
		if err != nil {
			return err
		}
		log.Info(service, "failedp99", failedReplicaMetrics[metric.ToMetricType(99)])

		// Compute the inflight request count for the service
		inflightRequestCount := requestCount - responseCount
		log.Info("Inflight request count for service", "service", service, "inflightRequestCount", inflightRequestCount)

		// Include the new inflight request count in the replica metrics
		err = prometheusMetrics.algo.ComputeNewAverage(float64(inflightRequestCount), successfulBucketsResult.timestamp, successfulReplicaMetrics[types.InFlightRequestCount])
		if err != nil {
			return err
		}

		// Get the violation metrics of the replica
		code200replicaViolations, err := prometheusMetrics.code200ViolationMetrics.GetReplicaSloViolationsByNameAndNamespace(service, namespace, log)
		if err != nil {
			return err
		}

		// Compute the new violations value
		log.Info(service, "totalCount", totalCount, "nonViolations", nonViolations)
		nViol := totalCount - nonViolations
		percentageViol := nViol / max(totalCount, 1) * 100

		// Include the new violation percentage in the replica violations
		err = prometheusMetrics.algo.ComputeNewAverage(percentageViol, nonViolationsBucket.Timestamp, code200replicaViolations[metric])
		if err != nil {
			return err
		}

		non200replicaViolations, err := prometheusMetrics.non200ViolationMetrics.GetReplicaSloViolationsByNameAndNamespace(service, namespace, log)
		if err != nil {
			return err
		}

		// Compute the new violations value
		log.Info(service, "failedTotalCount", failedResponseCount, "withinThreshold", failedResponseWithinThresholdCount)
		fnViol := failedResponseCount - failedResponseWithinThresholdCount
		fPercentageViol := fnViol / max(failedResponseCount, 1) * 100

		// Include the new violation percentage in the replica violations
		err = prometheusMetrics.algo.ComputeNewAverage(fPercentageViol, nonViolationsBucket.Timestamp, non200replicaViolations[metric])
		if err != nil {
			return err
		}

		log.Info(service, "within threshold", nonViolations, "count", totalCount, "failedCount", failedResponseCount, "violations", nViol, "percentage", percentageViol, "percentage violations after averaging", code200replicaViolations[metric])
	}

	return nil
}

func GetLatency(service string, namespace string, buckets QueryResult, count types.PrometheusBucket, percentile float64, timeoutCtx context.Context) (float64, error) {

	var sortedKeys []string
	SortKeys(*buckets.buckets, &sortedKeys)

	if len(sortedKeys) == 0 {
		log.Info("No keys found, returning infinity")
		return math.Inf(1), fmt.Errorf("No keys found for service %s in namespace %s", service, namespace)
	}

	log.Info("Calculating the P99 latency for service", "service", service)
	p99, err := ComputePercentile(types.Key{Name: service, Namespace: namespace}, *buckets.buckets, count, sortedKeys, 0.99)

	if err != nil {
		log.Error(err, "Could not compute P99 latency for service", "service", service, "namespace", namespace)
		return math.Inf(1), err
	}
	return p99, nil
}

func ComputePercentile(key types.Key, buckets map[string]types.PrometheusBucket, count types.PrometheusBucket, sortedKeys []string, percentile float64) (float64, error) {
	previousCount := 0.0
	previousLe := 0.0
	currentLe := 0.0

	log.Info("Computing percentile for key with count", "key", key, "buckets[\"+Inf\"].Count", buckets["+Inf"].Count)
	rank := percentile * float64(buckets["+Inf"].Count)
	log.Info("Calculating the percentile latency", "percentile", percentile, "rank", rank)

	if len(buckets) == 0 || len(buckets) == 1 || buckets["+Inf"].Count == 0 {
		log.Info("Insufficient data in buckets found, returning infinity")
		return math.Inf(1), fmt.Errorf("Insufficient data in buckets for key %v", key)
	}

	for _, le := range sortedKeys {
		bucket := buckets[le]
		if le == "+Inf" {
			currentLe = math.Inf(1)
		} else {
			currentLe, _ = strconv.ParseFloat(le, 64)
		}

		cumulCount := bucket.Count
		log.Info("Cumulative count for current bucket", "cumulCount", cumulCount)
		if rank <= cumulCount {
			log.Info("Rank is within the current bucket", "rank", rank, "previousCount", previousCount, "previousLe", previousLe, "currentLe", currentLe)
			bucketCount := cumulCount - previousCount
			percentileLatency := previousLe + (rank-previousCount)*(currentLe-previousLe)/bucketCount
			log.Info("Returning calculated latency for current bucket", "latency", percentileLatency)
			prometheusMetrics.previousReplicaLatencyHistograms[key] = types.PrometheusHistogram{
				Buckets: buckets,
				Latency: types.PrometheusLatency{P99: percentileLatency, Timestamp: buckets[le].Timestamp},
			}
			return percentileLatency, nil
		}

		previousCount = cumulCount
		previousLe = currentLe
	}

	err := fmt.Errorf("%v Could not compute percentile latency for key; previous count: %v, previous le: %v", key, previousCount, previousLe)
	return math.Inf(1), err
}

func GetBucketDelta(previousBuckets map[string]types.PrometheusBucket, newBuckets map[string]types.PrometheusBucket) map[string]types.PrometheusBucket {
	var delta = make(map[string]types.PrometheusBucket)

	for k, v := range newBuckets {
		if prev, ok := previousBuckets[k]; ok {
			delta[k] = types.PrometheusBucket{
				Le:        v.Le,
				Count:     math.Max(0, v.Count-prev.Count),
				Timestamp: v.Timestamp,
			}
		} else {
			delta[k] = v
		}
	}

	log.Info("GetBucketDelta", "Previous bucket total count:", previousBuckets["+Inf"].Count, "new bucket total count:", newBuckets["+Inf"].Count, "delta bucket total count", delta["+Inf"].Count)
	return delta
}

func GetResult(deployment string, query string, timeoutCtx context.Context) (QueryResult, error) {
	log.Info(deployment, "calling query", query)

	result, warnings, err := prometheusMetrics.prometheusApi.Query(timeoutCtx, query, time.Now())
	if err != nil {
		log.Error(err, "Error querying Prometheus")
		os.Exit(1)
	}

	if len(warnings) > 0 {
		log.Info("Warnings when querying Prometheus:", "warnings", warnings)
	}

	log.Info(deployment, "prometheusQueryResult", result.String())

	if result.Type() == model.ValVector {
		if vector, ok := result.(model.Vector); ok {
			if len(vector) == 0 {
				log.Info(deployment, "Result is an empty vector", vector)
				if strings.Contains(query, "bucket") {
					buckets := make(map[string]types.PrometheusBucket)
					buckets["+Inf"] = emptyBucket
					return QueryResult{buckets: &buckets, timestamp: time.Now().UTC()}, &NoDataReceived{Code: 666, Message: "Empty response was received", Query: query}
				} else {
					value := math.Inf(1)
					return QueryResult{value: &value, timestamp: time.Now().UTC()}, &NoDataReceived{Code: 666, Message: "Empty response was received", Query: query}
				}
			}

			var timestamp time.Time
			var buckets = make(map[string]types.PrometheusBucket)

			queryResult := QueryResult{
				buckets:   &buckets,
				timestamp: timestamp,
			}

			for _, sample := range vector {
				// If the sample does not have an 'le' label, it is the count or sum, a single value
				if _, ok := sample.Metric["le"]; !ok {
					value := float64(sample.Value)
					return QueryResult{value: &value, timestamp: sample.Timestamp.Time()}, nil
				}

				// Else, we are looking at the buckets
				le := string(sample.Metric["le"])
				timestamp = sample.Timestamp.Time()

				// The sample is a tuple of (timestamp, value)
				(*queryResult.buckets)[le] = types.PrometheusBucket{
					Le:        le,
					Count:     float64(sample.Value),
					Timestamp: timestamp,
				}
			}

			queryResult.timestamp = timestamp
			return queryResult, nil
		}

	} else if result.Type() == model.ValMatrix {
		if matrix, ok := result.(model.Matrix); ok {
			if len(matrix) == 0 {
				log.Info(deployment, "Result is an empty matrix", matrix)
				buckets := make(map[string]types.PrometheusBucket)
				buckets["0"] = emptyBucket
				return QueryResult{buckets: &buckets, timestamp: time.Now().UTC()}, &NoDataReceived{Code: 666, Message: "Empty response was received", Query: query}
			}
			var timestamp time.Time
			var buckets = make(map[string]types.PrometheusBucket)

			queryResult := QueryResult{
				buckets:   &buckets,
				timestamp: timestamp,
			}

			for _, sampleStream := range matrix {

				if len(sampleStream.Values) == 0 {
					continue
				}

				// The last value in the sample stream is the most recent one
				// and the sample pair is a tuple of (timestamp, value)
				samplePair := sampleStream.Values[len(sampleStream.Values)-1]
				le := string(sampleStream.Metric["le"])
				timestamp = samplePair.Timestamp.Time()

				(*queryResult.buckets)[le] = types.PrometheusBucket{
					Le:        le,
					Count:     float64(samplePair.Value),
					Timestamp: timestamp,
				}
			}

			queryResult.timestamp = timestamp
			return queryResult, nil
		}

	} else {
		return QueryResult{}, fmt.Errorf("The Prometheus response ValueType %s is not supported (yet)", result.Type().String())
	}

	return QueryResult{}, fmt.Errorf("There were no values for this metric for this deployment")
}

func (metr *PrometheusMetrics) RefreshValues() error {
	fd, ok := metr.algo.(*aggregation.ForwardDecay)
	// If we have a landmark and it is more than 20min ago
	if ok {
		for key, metrics := range metr.code200ViolationMetrics {
			for metric, values := range metrics {
				log.Info(key.Name, "metric", metric, "oldValues", values)
				err := fd.RescaleRelativeToLandmark(values)

				if err != nil {
					return err
				}
				log.Info(key.Name, "metric", metric, "newValues", values)
			}
		}
	}

	return nil
}

func GetActualSum(buckets map[string]types.PrometheusBucket) float64 {
	sum := 0.0
	for _, bucket := range buckets {
		sum += bucket.Count
	}
	return sum
}

func SortKeys(bucketDelta map[string]types.PrometheusBucket, sortedKeys *[]string) {

	for k := range bucketDelta {
		*sortedKeys = append(*sortedKeys, k)
	}

	sort.Slice(*sortedKeys, func(i, j int) bool {
		if (*sortedKeys)[i] == "+Inf" {
			return false // +Inf goes after everything
		}
		if (*sortedKeys)[j] == "+Inf" {
			return true
		}

		a, _ := strconv.ParseFloat((*sortedKeys)[i], 64)
		b, _ := strconv.ParseFloat((*sortedKeys)[j], 64)

		return a < b
	})

}
