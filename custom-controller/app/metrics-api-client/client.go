package metrics_api_client

import (
	pb "thesis/metrics-api-client/linkerd2/viz/metrics-api/gen/viz"

	"go.opencensus.io/plugin/ocgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	apiPort       = 8085
	apiDeployment = "metrics-api"
)

// NewInternalClient creates a new Viz API client intended to run inside a
// Kubernetes cluster.
func NewInternalClient(addr string) (pb.ApiClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(&ocgrpc.ClientHandler{}))
	if err != nil {
		return nil, err
	}

	return pb.NewApiClient(conn), nil
}
