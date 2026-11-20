package types

import (
	"math"
	"time"
)

type PrometheusBucket struct {
	Le        string
	Count     float64
	Timestamp time.Time
}

type PrometheusCount struct {
	Count     float64
	Timestamp time.Time
}

type PrometheusSum struct {
	Sum       float64
	Timestamp time.Time
}

type PrometheusLatency struct {
	P99       float64
	Timestamp time.Time
}

type PrometheusHistogram struct {
	// Buckets is a map where the key is the upper bound of the latency bucket (Le)
	// and the value is the corresponding PrometheusBucket containing the count and timestamp.
	Buckets map[string]PrometheusBucket
	Count   PrometheusCount
	Sum     PrometheusSum
	Latency PrometheusLatency
}

var initPrometheusHistogram = PrometheusHistogram{
	Buckets: map[string]PrometheusBucket{
		"+Inf": {Le: "+Inf", Count: 0, Timestamp: time.Now()},
	},
	Count:   PrometheusCount{Count: 0, Timestamp: time.Now()},
	Sum:     PrometheusSum{Sum: 0, Timestamp: time.Now()},
	Latency: PrometheusLatency{P99: math.Inf(1), Timestamp: time.Now()},
}

type ReplicaLatencyHistogramList map[Key]PrometheusHistogram

func InitPrometheusHistogram() PrometheusHistogram {
	return initPrometheusHistogram
}
