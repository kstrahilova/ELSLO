package metrics

import (
	"context"
	"sync"
	"thesis/types"

	"github.com/go-logr/logr"
)

type metrRead struct {
	// reachability
	R bool
	//metrics-api
	M bool
	// prometheus
	P bool
}

type ReplicaUptimeMetrics map[types.Key]metrRead

var (
	log           logr.Logger
	emptyCtx      context.Context
	uptimeOnce    sync.Once
	uptimeMu      sync.Mutex
	uptimeMetrics ReplicaUptimeMetrics
)

func GetUptimeMetrics() ReplicaUptimeMetrics {
	uptimeOnce.Do(func() {
		uptimeMetrics = make(map[types.Key]metrRead)
	})

	return uptimeMetrics
}

func (metr ReplicaUptimeMetrics) InitUptime(name string, namespace string) {
	metr[types.Key{Name: name, Namespace: namespace}] = metrRead{R: false, P: false, M: false}
}

func (metr ReplicaUptimeMetrics) UptimeMetricsExists(name string, namespace string) bool {
	_, ok := metr[types.Key{Name: name, Namespace: namespace}]

	return ok
}

func (metr ReplicaUptimeMetrics) GetUptimeM(name string, namespace string) bool {
	return metr[types.Key{Name: name, Namespace: namespace}].M
}

func (metr ReplicaUptimeMetrics) SetUptimeM(name string, namespace string, M bool) {
	metr[types.Key{Name: name, Namespace: namespace}] = metrRead{
		R: metr[types.Key{Name: name, Namespace: namespace}].R,
		M: M,
		P: metr[types.Key{Name: name, Namespace: namespace}].P,
	}
}

func (metr ReplicaUptimeMetrics) GetUptimeP(name string, namespace string) bool {
	return metr[types.Key{Name: name, Namespace: namespace}].P
}

func (metr ReplicaUptimeMetrics) SetUptimeP(name string, namespace string, P bool) {
	metr[types.Key{Name: name, Namespace: namespace}] = metrRead{
		R: metr[types.Key{Name: name, Namespace: namespace}].R,
		M: metr[types.Key{Name: name, Namespace: namespace}].M,
		P: P,
	}
}

func (metr ReplicaUptimeMetrics) GetUptimeR(name string, namespace string) bool {
	return metr[types.Key{Name: name, Namespace: namespace}].R
}

func (metr ReplicaUptimeMetrics) SetUptimeR(name string, namespace string, R bool) {
	metr[types.Key{Name: name, Namespace: namespace}] = metrRead{
		R: R,
		M: metr[types.Key{Name: name, Namespace: namespace}].M,
		P: metr[types.Key{Name: name, Namespace: namespace}].P,
	}
}

func Btof64(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}
