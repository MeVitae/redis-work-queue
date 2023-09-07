package interfaces

import "context"

type Deployments interface {
	// Get a deployment by name.
	GetDeployment(ctx context.Context, name string) (Deployment, error)
}

type Deployment interface {
	// GetReady returns the number of currently active workers.
	GetReady(context.Context) (count int32, err error)
	// GetRequest returns the number of requested workers.
	GetRequest(context.Context) (count int32, err error)
	// SetRequest requests the specified number of workers.
	SetRequest(ctx context.Context, count int32) error
}
