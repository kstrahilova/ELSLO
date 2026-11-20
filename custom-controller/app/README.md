# ELSLO Controller Source

This directory contains the Go source compiled by the Helm chart's builder
init container. It is not a separate deployment package. For prerequisites,
installation, configuration, and the smoke-test procedure, see the
[submission guide](../../README.md).

## What the controller does

The controller watches the configured Traefik `IngressRoute`, reads Linkerd Viz
Prometheus and metrics API data, estimates backend Service weights, and updates
the route. The selected estimator and aggregation algorithm are configured by
the chart for each `testingEnvironments` entry.

The controller currently expects exactly one route in the managed
`IngressRoute`. The watched route name is
`<APPLICATION_TO_MANAGE>-<environment>-route`; the submitted chart defaults to
`hotel-reservation-frontend-gl-route`. It also checks each backend at
`http://<service>.<namespace>.svc.cluster.local:8081/health`.

## Source map

| Path | Responsibility |
| --- | --- |
| `main.go` | Reads configuration, sets up the scheme and controller manager, and starts reconciliation. |
| `controller/ingressroute_controller.go` | Reconciles the managed route and writes computed weights. |
| `latency-estimation/` | ELSLO, ELS, and L3 estimation implementations. |
| `aggregation-algo/` | Forward-decay and EWMA aggregation. |
| `metrics/` | Prometheus, Linkerd metrics API, and backend reachability data. |
| `metrics-api-client/` | Linkerd Viz gRPC client and protobuf definition. |
| `types/` | Shared configuration, metrics, histogram, and weighted-Service types. |

The chart mounts these files through ConfigMaps and regenerates the Linkerd
metrics API protobuf Go bindings during the builder init-container step.