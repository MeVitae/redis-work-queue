// Package workqueue is the Go implementation of a work queue, on top of a redis database, with implementations in Python, Rust, Go and C#.
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
		leaseKey:      name.Concat(":leased_by_session:"),
		itemDataKey:   name.Concat(":item:"),
	}
}

// AddItemToPipeline adds an item to the work queue. This adds the redis commands onto the pipeline passed.
//
// Use [WorkQueue.AddItem] if you don't want to pass a pipeline directly.
func (workQueue *WorkQueue) AddItemToPipeline(ctx context.Context, pipeline redis.Pipeliner, item Item) {
	// Add the item data
	// NOTE: it's important that the data is added first, otherwise someone could pop the item
	// before the data is ready
	pipeline.Set(ctx, workQueue.itemDataKey.Of(item.ID), item.Data, never)
	// Then add the id to the work queue
	pipeline.LPush(ctx, workQueue.mainQueueKey, item.ID)
}

// AddItem to the work queue.
//
// This creates a pipeline and executes it on the database.
func (workQueue *WorkQueue) AddItem(ctx context.Context, db *redis.Client, item Item) error {
	pipeline := db.Pipeline()
	workQueue.AddItemToPipeline(ctx, pipeline, item)
	_, err := pipeline.Exec(ctx)
	return err
}

// AddNewItem to the work Queue.
//
// This function allows the adding of item to queue atomically. Using Watch it keeps trying to execute
// addItem untill there is no change within the queues, verifications have been done and the addItem has been fully executed.
// Therefore this wont allow duplifications of items within the queues
// This method should be used only when database is locked it could break other redis commands.
//
// Returns nil if successful  otherwise the error.
func (workQueue *WorkQueue) AddNewItem(ctx context.Context, db *redis.Client, item Item) error {
	txf := func(tx *redis.Tx) error {

		pipeNew := db.Pipeline()

		processingItemsInQueueCmd := pipeNew.LPos(ctx, workQueue.processingKey, item.ID, redis.LPosArgs{
			Rank:   0,
			MaxLen: 0,
		})

		workingItemsInQueueCmd := pipeNew.LPos(ctx, workQueue.mainQueueKey, item.ID, redis.LPosArgs{
			Rank:   0,
			MaxLen: 0,
		})

		_, err := pipeNew.Exec(ctx)

		_, ProcessingQueueCheck := processingItemsInQueueCmd.Result()
		_, WorkingQueueCheck := workingItemsInQueueCmd.Result()
		if ProcessingQueueCheck == redis.Nil && WorkingQueueCheck == redis.Nil {

			pipeEx := tx.Pipeline()
			workQueue.AddItemToPipeline(ctx, pipeEx, item)
			_, err = pipeEx.Exec(ctx)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for {
		err := db.Watch(ctx, txf, workQueue.mainQueueKey, workQueue.processingKey)
		// Retry on transaction failure, return on anything else.
		if err != redis.TxFailedErr {
			return err
		}
	}
}

// Lengths gets you the lengths of the lists atomically.
//
// This can be used to get the real number of items within the main and processing queue
func (workQueue *WorkQueue) Lengths(ctx context.Context, db *redis.Client) (queueLen, processingLen int64, err error) {
	tx := db.TxPipeline()

	queueLenPipe := tx.LLen(ctx, workQueue.mainQueueKey)
	processingLenPipe := tx.LLen(ctx, workQueue.processingKey)

	_, err = tx.Exec(ctx)
	if err == nil {
		queueLen, err = queueLenPipe.Result()
	}
	if err == nil {
		processingLen, err = processingLenPipe.Result()
	}
	return
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

// Complete marks a job as completed and remove it from the work queue. After Complete has been
// called (and returns true), no workers will receive this job again.
//
// Complete returns a boolean indicating if *the job has been removed* **and** *this worker was the
// first worker to call Complete*. So, while lease might give the same job to multiple workers,
// complete will return true for only one worker.
func (workQueue *WorkQueue) Complete(ctx context.Context, db *redis.Client, item *Item) (bool, error) {
	removed, err := db.LRem(ctx, workQueue.processingKey, 0, item.ID).Result()
	if removed == 0 || err != nil {
		return false, err
	}
	// If we did actually remove it, delete the item data and lease.
	// If we didn't really remove it, it's probably been returned to the work queue so the data is
	// still needed and the lease might not be ours (if it is still ours, it'll expire anyway).
	_, err = db.Pipelined(ctx, func(pipeline redis.Pipeliner) error {
		pipeline.Del(ctx, workQueue.itemDataKey.Of(item.ID))
		pipeline.Del(ctx, workQueue.leaseKey.Of(item.ID))
		return nil
	})
	return true, err
}

func sliceContainsString(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}
