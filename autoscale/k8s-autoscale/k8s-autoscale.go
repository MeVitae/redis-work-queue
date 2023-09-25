// package k8sautoscale implements Deployments and Deployment interface for a Kubernetes cluster.
package k8sautoscale

import (
	"context"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	workqueue "github.com/mevitae/redis-work-queue/go"
	"github.com/redis/go-redis/v9"

	"github.com/mevitae/redis-work-queue/autoscale/interfaces"
	scale "github.com/mevitae/redis-work-queue/autoscale/k8s-scale"
	"github.com/mevitae/redis-work-queue/autoscale/wqautoscale"
)

// WorkQueues provides an interface allowing work queues to be generated by name.
type WorkQueues struct {
	NamePrefix workqueue.KeyPrefix
	DB         *redis.Client
}

func NewWorkQueues(namePrefix string, db *redis.Client) *WorkQueues {
	return &WorkQueues{
		NamePrefix: workqueue.KeyPrefix(namePrefix),
		DB:         db,
	}
}

func (queues *WorkQueues) GetWorkQueue(name string) wqautoscale.WorkQueue {
	return &WrappedWorkQueue{
		workQueue: workqueue.NewWorkQueue(queues.NamePrefix.Concat(name)),
		db:        queues.DB,
	}
}

// WrappedWorkQueue is a WorkQueue, wrapped with a redis database, providing the TotalItems method.
type WrappedWorkQueue struct {
	workQueue workqueue.WorkQueue
	db        *redis.Client
}

func (wrapped *WrappedWorkQueue) Counts(ctx context.Context) (queueLen, processing int32, err error) {
	// FIXME: Get these atomically
	var queueLen64 int64
	queueLen64, err = wrapped.workQueue.QueueLen(ctx, wrapped.db)
	queueLen = int32(queueLen64)
	if err != nil {
		return
	}

	var processing64 int64
	processing64, err = wrapped.workQueue.Processing(ctx, wrapped.db)
	processing = int32(processing64)

	return
}

type Config struct {
	// Namespace of the k8s deployments.
	Namespace string
	// Redis options for accessing work queues.
	Redis redis.Options
	// QueueNamePrefix is the prefix used for generating queue names.
	QueueNamePrefix string
	// SegmentedDeployments determines if the k8s deployments interface should be wrapped in `SegmentedDeployments`.
	//
	// This allows deployment counts to be segmented.
	SegmentedDeployments bool `yaml:"segmentedDeployments"`
	// Autoscale config.
	Autoscale wqautoscale.Config
}

func LoadConfig(configPath string) (Config, error) {
	var config Config
	f, err := os.Open(configPath)
	if err != nil {
		return config, fmt.Errorf("failed to open config file: %w", err)
	}
	err = yaml.NewDecoder(f).Decode(&config)
	if err != nil {
		return config, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
	}
	return config, nil
}

// InClusterAutoscaler creates an autoscaler for scaling deployments within the cluster the process
// is running within.
func InClusterAutoscaler(
	ctx context.Context,
	configPath string,
	time int64,
	scaleReporter interfaces.ScaleReporter,
) (*wqautoscale.AutoScale, error) {
	config, err := LoadConfig(configPath)
	if err != nil {
		return nil, err
	}

	db := redis.NewClient(&config.Redis)
	workQueues := NewWorkQueues(config.QueueNamePrefix, db)

	deployments, err := scale.InClusterDeployments(config.Namespace)
	if err != nil {
		return nil, err
	}
	var deploymentsInterface interfaces.Deployments = deployments
	if config.SegmentedDeployments {
		deploymentsInterface = interfaces.NewSegmentedDeployments(deploymentsInterface)
	}

	return wqautoscale.NewAutoScale(
		ctx,
		workQueues,
		deploymentsInterface,
		config.Autoscale,
		time,
		scaleReporter,
	)
}
