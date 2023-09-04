package interfaces

type Deployments interface {
	GetDeployment(string) Deployment
}

type Deployment interface {
	GetRequest() int32
	GetReady() int32

	SetRequest(int32)
}
