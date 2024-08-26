// Package workqueue is the Go implementation of a work queue, on top of a redis database, with implementations in Python, Rust, Go, Node.js (TypeScript) and Dotnet (C#).
//
// For an overview of how the work queue works, it’s limitations, and the general concepts and
// implementations in other languages, please read the redis-work-queue readme at
// https://github.com/MeVitae/redis-work-queue/blob/main/README.md.
//
// # Setup
//
//	import (
//	    "github.com/redis/go-redis/v9"
//	    workqueue "github.com/mevitae/redis-work-queue/go"
//	)
//
//	db := redis.NewClient(&redis.Options{
//	    Addr: "your-redis-server:1234",
//	})
//	workQueue := workqueue.NewWorkQueue(workqueue.KeyPrefix("example_work_queue"))
//
// # Adding Items
//
//	// From bytes:
//	exampleItem := NewItem([]byte("[1,2,3]"))
//	// Or from JSON:
//	jsonItem, err := NewItemFromJSONData([1, 2, 3])
//	// ...
//	err = workQueue.AddItem(ctx, db, jsonItem)
//
// # Completing work
//
// Please read the documentation on leasing and completing items at
// https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item
//
//	for (;;) {
//	   job, err := workQueue.Lease(ctx, db, true, 0, 5*time.Second)
//	   if err != nil { panic(err) }
//	   doSomeWork(job)
//	   _, err = workQueue.Complete(ctx, db, job)
//	   if err != nil { panic(err) }
//	}
//
// # Handling errors
//
// Please read the documentation on handling errors at
// https://github.com/MeVitae/redis-work-queue/blob/main/README.md#handling-errors
//
//	for (;;) {
//	   job, err := workQueue.Lease(ctx, db, true, 0, 5*time.Second)
//	   if err != nil { panic(err) }
//	   err = doSomeWork(job)
//	   if err == nil {
//	       // Mark successful jobs as complete
//	       _, err = workQueue.Complete(ctx, db, job)
//	       if err != nil { panic(err) }
//	   } else if !shouldRetryAfter(err) {
//	       // Errors that shouldn't cause a retry should mark the job as complete so it isn't tried
//	       // again.
//	       logError(err)
//	       _, err = workQueue.Complete(ctx, db, job)
//	       if err != nil { panic(err) }
//	   } else {
//	       // Drop a job that should be retried - it will be returned to the work queue after the
//	       // (5 second) lease expires.
//	   }
//	}
package workqueue

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const never time.Duration = 0

// WorkQueue backed by a redis database.
type WorkQueue struct {
	// session is a unique ID for this instance
	session string
	// mainQueueKey is the key for the list of items in the queue
	mainQueueKey string
	/// processingKey is the key for the list of items being processed
	processingKey string
	// leaseKey is the  key prefix for lease entries
	leaseKey KeyPrefix
	// itemDataKey is the key prefix for item data
	itemDataKey KeyPrefix
}

func NewWorkQueue(name KeyPrefix) WorkQueue {
	return WorkQueue{
		session:       uuid.NewString(),
		mainQueueKey:  name.Of(":queue"),
		processingKey: name.Of(":processing"),
		leaseKey:      name.Concat(":lease:"),
		itemDataKey:   name.Concat(":item:"),
	}
}

// AddItem to the work queue.
//
// If an item with the same ID already exists, this item is not added, and false is returned. Otherwise, if the item is added true is returned.
//
// If you know the item ID is unique, and not already in the queue, use the optimised WorkQueue.AddUniqueItem instead.
func (workQueue *WorkQueue) AddItem(ctx context.Context, db *redis.Client, item Item) (bool, error) {
	added, err := db.SetNX(ctx, workQueue.itemDataKey.Of(item.ID), item.Data, never).Result()
	if added {
		err = db.LPush(ctx, workQueue.mainQueueKey, item.ID).Err()
	}
	return added, err
}

// AddItemToPipeline adds an item, which is known to have an ID not already in the queue, to the work queue. This adds the redis commands onto the pipeline passed.
//
// Use [WorkQueue.AddUniqueItem] if you don't want to pass a pipeline directly.
func (workQueue *WorkQueue) AddUniqueItemToPipeline(ctx context.Context, pipeline redis.Pipeliner, item Item) {
	// Add the item data
	// NOTE: it's important that the data is added first, otherwise someone could pop the item
	// before the data is ready
	pipeline.Set(ctx, workQueue.itemDataKey.Of(item.ID), item.Data, never)
	// Then add the id to the work queue
	pipeline.LPush(ctx, workQueue.mainQueueKey, item.ID)
}

// AddItem, which is known to have an ID not already in the queue, to the work queue.
//
// This creates a pipeline and executes it on the database.
func (workQueue *WorkQueue) AddUniqueItem(ctx context.Context, db *redis.Client, item Item) error {
	pipeline := db.Pipeline()
	workQueue.AddUniqueItemToPipeline(ctx, pipeline, item)
	_, err := pipeline.Exec(ctx)
	return err
}

// Return the length of the work queue (not including items being processed, see
// [WorkQueue.Processing]).
func (workQueue *WorkQueue) QueueLen(ctx context.Context, db *redis.Client) (int64, error) {
	return db.LLen(ctx, workQueue.mainQueueKey).Result()
}

// Processing returns the number of items being processed.
func (workQueue *WorkQueue) Processing(ctx context.Context, db *redis.Client) (int64, error) {
	return db.LLen(ctx, workQueue.processingKey).Result()
}

// Request a work lease the work queue. This should be called by a worker to get work to complete.
// When completed, the [WorkQueue.Complete] method should be called.
//
// If block is true, the function will return either when a job is leased or after timeout if
// timeout isn't 0.
//
// If the job is not completed before the end of lease_duration, another worker may pick up the same
// job. It is not a problem if a job is marked as done more than once.
//
// If no job is available before the timeout, (nil, nil) is returned.
//
// If you've not already done it, it's worth reading the documentation on leasing items at
// https://github.com/MeVitae/redis-work-queue/blob/main/README.md#leasing-an-item
func (workQueue *WorkQueue) Lease(
	ctx context.Context,
	db *redis.Client,
	block bool,
	timeout time.Duration,
	leaseDuration time.Duration,
) (*Item, error) {
	for {
		// First, to get an item, we try to move an item from the main queue to the processing list.
		var command *redis.StringCmd
		if block {
			command = db.BRPopLPush(ctx, workQueue.mainQueueKey, workQueue.processingKey, timeout)
		} else {
			command = db.RPopLPush(ctx, workQueue.mainQueueKey, workQueue.processingKey)
		}
		itemId, err := command.Result()
		if itemId == "" || err != nil {
			// A nil error indicates no job available
			if err == redis.Nil {
				return nil, nil
			}
			return nil, err
		}

		// Get the item's data
		data, err := db.Get(ctx, workQueue.itemDataKey.Of(itemId)).Bytes()
		if err != nil {
			if err == redis.Nil {
				if block && timeout == 0 {
					continue
				}
				return nil, nil
			}
			return nil, err
		}

		// Now setup the lease item.
		// NOTE: Racing for a lease is ok
		err = db.SetEx(ctx, workQueue.leaseKey.Of(itemId), workQueue.session, leaseDuration).Err()

		return &Item{
			ID:   itemId,
			Data: data,
		}, err
	}
}

func (workQueue *WorkQueue) leaseExists(ctx context.Context, db *redis.Client, itemId string) (bool, error) {
	exists, err := db.Exists(ctx, workQueue.itemDataKey.Of(itemId)).Result()
	return exists > 0, err
}

func (workQueue *WorkQueue) LightClean(ctx context.Context, db *redis.Client) error {
	// A light clean only checks items in the processing queue
	itemIds, err := db.LRange(ctx, workQueue.processingKey, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("failed to list processing queue: %w", err)
	}
	for _, itemId := range itemIds {
		leaseExists, err := workQueue.leaseExists(ctx, db, itemId)
		if err != nil {
			return fmt.Errorf("failed to check if lease exists for %s: %w", itemId, err)
		}
		// If there's no lease for the item, then it should be reset.
		if !leaseExists {
			// We also check that the item actually exists before pushing it back to the main queue
			itemExists, err := db.Exists(ctx, workQueue.itemDataKey.Of(itemId)).Result()
			if err != nil {
				return fmt.Errorf("failed to check if item %s exists: %w", itemId, err)
			}
			if itemExists != 0 {
				fmt.Println(itemId, "has no lease, it will be reset")
				pipeline := db.Pipeline()
				pipeline.LRem(ctx, workQueue.processingKey, 0, itemId)
				pipeline.LPush(ctx, workQueue.mainQueueKey, itemId)
				_, err := pipeline.Exec(ctx)
				if err != nil {
					return fmt.Errorf("failed to move %s from processing queue to main queue: %w", itemId, err)
				}
			} else {
				fmt.Println(itemId, "was in the processing queue but does not exist")
				err = db.LRem(ctx, workQueue.processingKey, 0, itemId).Err()
				if err != nil {
					return fmt.Errorf("failed to remove %s from processing queue: %w", itemId, err)
				}
			}
		}
	}
	return nil
}

func (workQueue *WorkQueue) DeepClean(ctx context.Context, db *redis.Client) error {
	// A deep clean checks all data keys
	pipeline := db.Pipeline()
	itemDataKeys := db.Keys(ctx, workQueue.itemDataKey.Of("*"))
	mainQueueRes := db.LRange(ctx, workQueue.mainQueueKey, 0, -1)
	_, err := pipeline.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to list item data keys: %w", err)
	}
	mainQueue := mainQueueRes.Val()
	for _, itemDataKey := range itemDataKeys.Val() {
		itemId := itemDataKey[:len(workQueue.itemDataKey)]
		leaseExists, err := workQueue.leaseExists(ctx, db, itemId)
		if err != nil {
			return fmt.Errorf("failed to check if lease exists for %s: %w", itemId, err)
		}
		// If the item isn't in the queue, and there's no lease for the item, then it should be reset.
		if !slices.Contains(mainQueue, itemId) && !leaseExists {
			fmt.Println(itemId, "has no lease, it will be reset")
			pipeline := db.Pipeline()
			pipeline.LRem(ctx, workQueue.processingKey, 0, itemId)
			pipeline.LPush(ctx, workQueue.mainQueueKey, itemId)
			_, err := pipeline.Exec(ctx)
			if err != nil {
				return fmt.Errorf("failed to move %s from processing queue to main queue: %w", itemId, err)
			}
		}
	}
	return nil
}

// Complete marks a job as completed and remove it from the work queue. After Complete has been
// called (and returns true), no workers will receive this job again.
//
// Complete returns a boolean indicating if *the job has been removed* **and** *this worker was the
// first worker to call Complete*. So, while lease might give the same job to multiple workers,
// complete will return true for only one worker.
func (workQueue *WorkQueue) Complete(ctx context.Context, db *redis.Client, item *Item) (bool, error) {
	pipeline := db.Pipeline()
	delResult := pipeline.Del(ctx, workQueue.itemDataKey.Of(item.ID))
	pipeline.LRem(ctx, workQueue.processingKey, 0, item.ID)
	pipeline.Del(ctx, workQueue.leaseKey.Of(item.ID))
	_, err := pipeline.Exec(ctx)
	return delResult.Val() != 0, err
}
