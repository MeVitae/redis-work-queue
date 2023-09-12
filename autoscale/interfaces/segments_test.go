package interfaces

import (
	"context"
	"fmt"
	"testing"
)

type DummyDeployments struct {
	deployments map[string]*int32
}

type DummyDeployment struct {
	count *int32
}

func NewDummyDeployments() *DummyDeployments {
	return &DummyDeployments{
		deployments: make(map[string]*int32),
	}
}

func (deployments *DummyDeployments) GetDeployment(_ context.Context, name string) (Deployment, error) {
	count, ok := deployments.deployments[name]
	if !ok {
		c := int32(0)
		count = &c
		deployments.deployments[name] = count
	}
	return DummyDeployment{count}, nil
}

func (deployment DummyDeployment) GetReady(_ context.Context) (int32, error) {
	return *deployment.count, nil
}

func (deployment DummyDeployment) GetRequest(_ context.Context) (int32, error) {
	return *deployment.count, nil
}

func (deployment DummyDeployment) SetRequest(_ context.Context, count int32) error {
	*deployment.count = count
	return nil
}

func unwrap[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}
	return value
}

func assertEq(a, b int32) {
	if a != b {
		panic(fmt.Errorf("%v != %v", a, b))
	}
}

func TestSegmentedDeployments(t *testing.T) {
	ctx := context.Background()
	deps := NewSegmentedDeployments(NewDummyDeployments())

	dep0a := unwrap(deps.GetDeployment(ctx, "0"))
	dep0b := unwrap(deps.GetDeployment(ctx, "0"))
	dep1a := unwrap(deps.GetDeployment(ctx, "1"))
	dep1b := unwrap(deps.GetDeployment(ctx, "1/b"))
	dep1c := unwrap(deps.GetDeployment(ctx, "1/c"))

	assertEq(unwrap(dep0a.GetRequest(ctx)), 0)
	assertEq(unwrap(dep0a.GetReady(ctx)), 0)
	assertEq(unwrap(dep0b.GetRequest(ctx)), 0)
	assertEq(unwrap(dep0b.GetReady(ctx)), 0)
	assertEq(unwrap(dep1a.GetRequest(ctx)), 0)
	assertEq(unwrap(dep1a.GetReady(ctx)), 0)
	assertEq(unwrap(dep1b.GetRequest(ctx)), 0)
	assertEq(unwrap(dep1b.GetReady(ctx)), 0)
	assertEq(unwrap(dep1c.GetRequest(ctx)), 0)
	assertEq(unwrap(dep1c.GetReady(ctx)), 0)

	dep0a.SetRequest(ctx, 5)
	assertEq(unwrap(dep0a.GetRequest(ctx)), 5)
	assertEq(unwrap(dep0a.GetReady(ctx)), 5)
	assertEq(unwrap(dep0b.GetRequest(ctx)), 5)
	assertEq(unwrap(dep0b.GetReady(ctx)), 5)

	dep0b.SetRequest(ctx, 2)
	assertEq(unwrap(dep0a.GetRequest(ctx)), 2)
	assertEq(unwrap(dep0a.GetReady(ctx)), 2)
	assertEq(unwrap(dep0b.GetRequest(ctx)), 2)
	assertEq(unwrap(dep0b.GetReady(ctx)), 2)

	dep1b.SetRequest(ctx, 2)
	assertEq(unwrap(dep1a.GetRequest(ctx)), 0)
	assertEq(unwrap(dep1a.GetReady(ctx)), 0)
	assertEq(unwrap(dep1b.GetRequest(ctx)), 2)
	assertEq(unwrap(dep1b.GetReady(ctx)), 2)
	assertEq(unwrap(dep1c.GetRequest(ctx)), 0)
	assertEq(unwrap(dep1c.GetReady(ctx)), 0)

	dep1b.SetRequest(ctx, 3)
	assertEq(unwrap(dep1a.GetRequest(ctx)), 0)
	assertEq(unwrap(dep1a.GetReady(ctx)), 0)
	assertEq(unwrap(dep1b.GetRequest(ctx)), 3)
	assertEq(unwrap(dep1b.GetReady(ctx)), 3)
	assertEq(unwrap(dep1c.GetRequest(ctx)), 0)
	assertEq(unwrap(dep1c.GetReady(ctx)), 0)

	dep1a.SetRequest(ctx, 1)
	assertEq(unwrap(dep1a.GetRequest(ctx)), 1)
	assertEq(unwrap(dep1a.GetReady(ctx)), 1)
	assertEq(unwrap(dep1b.GetRequest(ctx)), 3)
	assertEq(unwrap(dep1b.GetReady(ctx)), 3)
	assertEq(unwrap(dep1c.GetRequest(ctx)), 0)
	assertEq(unwrap(dep1c.GetReady(ctx)), 0)

	dep1c.SetRequest(ctx, 6)
	assertEq(unwrap(dep1a.GetRequest(ctx)), 1)
	assertEq(unwrap(dep1a.GetReady(ctx)), 1)
	assertEq(unwrap(dep1b.GetRequest(ctx)), 3)
	assertEq(unwrap(dep1b.GetReady(ctx)), 3)
	assertEq(unwrap(dep1c.GetRequest(ctx)), 6)
	assertEq(unwrap(dep1c.GetReady(ctx)), 6)

	dep1a.SetRequest(ctx, 9)
	dep1c.SetRequest(ctx, 0)
	assertEq(unwrap(dep1a.GetRequest(ctx)), 9)
	assertEq(unwrap(dep1a.GetReady(ctx)), 9)
	assertEq(unwrap(dep1b.GetRequest(ctx)), 3)
	assertEq(unwrap(dep1b.GetReady(ctx)), 3)
	assertEq(unwrap(dep1c.GetRequest(ctx)), 0)
	assertEq(unwrap(dep1c.GetReady(ctx)), 0)
}
