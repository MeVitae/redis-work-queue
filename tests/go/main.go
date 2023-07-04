package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/bsm/redislock"
	workqueue "github.com/mevitae/redis-work-queue/go"
	"github.com/redis/go-redis/v9"
)

type SharedJobData struct {
	A int `json:"a"`
	B int `json:"b"`
}

type SharedJobResult struct {
	A      int    `json:"a"`
	Sum    int    `json:"sum"`
	Prod   int    `json:"prod"`
	Worker string `json:"worker"`
}

func main() {
	if len(os.Args) < 2 {
		panic("first command line argument must be redis host")
	}
	fmt.Println("-----")
	db := redis.NewClient(&redis.Options{
		Addr: os.Args[1],
	})

	ctx := context.Background()

	keyPrefix := workqueue.KeyPrefix("Duplicate-Items-Test")
	workQueue := workqueue.NewWorkQueue(keyPrefix)

	fmt.Println("Duplicated jobs test started")
	passed := 0
	notPassed := 0
	numberOfTests := 150
	numberOfRandomPossibleItems := 35
	items := make([]workqueue.Item, 0)

	for i := 1; i <= numberOfTests; i++ {
		itemData1 := strconv.Itoa(rand.Intn(numberOfRandomPossibleItems))
		lockDuration := 100 * time.Millisecond
		itemData2 := strconv.Itoa(rand.Intn(numberOfRandomPossibleItems))
		item1 := workqueue.Item{
			Data: []byte(itemData1),
			ID:   itemData1,
		}
		item2 := workqueue.Item{
			Data: []byte(itemData2),
			ID:   itemData2,
		}
		items = append(items, item1, item2)

		lockKey := fmt.Sprintf("lock:%s", item1.ID)

		locker := redislock.New(db)

		// Acquire lock for item.ID
		lock, _ := locker.Obtain(ctx, lockKey, lockDuration, nil)
		defer func() {
			// Release the lock for item.ID
			err := lock.Release(ctx)
			if err != nil {
				fmt.Printf("failed to release lock for item.ID: %v\n", err)
			}
		}()

		// Process item.ID
		result1 := workQueue.AddAtomicItem(ctx, db, item1)
		waitTime := 100 * time.Millisecond
		fmt.Printf("Waiting for %v before proceeding to the next operation\n", waitTime)
		time.Sleep(waitTime)
		// Acquire lock for item2.ID
		lockKey2 := fmt.Sprintf("lock:%s", item2.ID)
		lock2, _ := locker.Obtain(ctx, lockKey2, lockDuration, nil)
		defer func() {
			// Release the lock for item2.ID
			err := lock2.Release(ctx)
			if err != nil {
				fmt.Printf("failed to release lock for item2.ID: %v\n", err)
			}
		}()

		// Process item2.ID
		result2 := workQueue.AddAtomicItem(ctx, db, item2)

		if result1 == nil {
			passed++
		} else {
			notPassed++
		}

		if result2 == nil {
			passed++
		} else {
			notPassed++
		}

		if rand.Intn(10) < 3 {
			workQueue.Lease(ctx, db, true, time.Second*4, time.Second*1)
		}

		if rand.Intn(10) < 5 {
			toRemove := &items[len(items)-1]
			items = items[:len(items)-1]
			workQueue.Complete(ctx, db, toRemove)
		}
	}

	expectedTotal := numberOfTests * 2
	if notPassed+passed != expectedTotal {
		panic(fmt.Sprintf("The number of passed items (%d) with the number of not passed items (%d) should be equal to the number of tests * 2: %d", passed, notPassed, expectedTotal))
	}

	mainQueueKey := keyPrefix.Of(":queue")
	processingKey := keyPrefix.Of(":processing")
	mainItems := db.LRange(ctx, mainQueueKey, 0, -1).Val()
	processingItems := db.LRange(ctx, processingKey, 0, -1).Val()
	fmt.Println(mainItems, "------", processingItems)
	for _, item := range mainItems {

		if sliceContainsString(processingItems, item) {
			fmt.Println(processingItems, "---", item)
			panic("Found duplicated item from processing queue inside main queue")
		}
	}

	for _, item := range processingItems {
		if sliceContainsString(mainItems, item) {
			panic("Found duplicated item from processing queue inside main queue")
		}
	}
	fmt.Println("Replicated items test finished")

	goResultsKey := workqueue.KeyPrefix("results:go:")
	sharedResultsKey := workqueue.KeyPrefix("results:shared:")

	goQueue := workqueue.NewWorkQueue(workqueue.KeyPrefix("go_jobs"))
	sharedQueue := workqueue.NewWorkQueue(workqueue.KeyPrefix("shared_jobs"))

	goJobCounter := 0
	sharedJobCounter := 0

	shared := false
	for {
		shared = !shared
		fmt.Println(goQueue.GetQueueLengths(ctx, db))
		fmt.Println(sharedQueue.GetQueueLengths(ctx, db))
		if shared {
			sharedJobCounter++

			// First, try to get a job from the shared job queue
			block := sharedJobCounter%5 == 0
			fmt.Println("Leasing shared with block =", block)
			job, err := sharedQueue.Lease(ctx, db, block, time.Second, 2*time.Second)
			if err != nil {
				panic(err)
			}
			// If there was no job, continue.
			// Also, if we get 'unlucky', crash while completing the job.
			if job == nil || sharedJobCounter%7 == 0 {
				fmt.Println("Dropping job:", job)
				continue
			}

			// Parse the data
			data, err := workqueue.ItemDataJson[SharedJobData](job)
			if err != nil {
				panic(err)
			}
			// Generate the response
			result := SharedJobResult{
				A:      data.A,
				Sum:    data.A + data.B,
				Prod:   data.A * data.B,
				Worker: "go",
			}
			resultJson, err := json.Marshal(result)
			if err != nil {
				panic(err)
			}
			fmt.Println("Result:", result)
			// Pretend it takes us a while to compute the result
			// Sometimes this will take too long and we'll timeout
			if sharedJobCounter%12 == 0 {
				time.Sleep(time.Second * time.Duration(sharedJobCounter%4))
			}

			// Store the result
			err = db.Set(ctx, sharedResultsKey.Of(job.ID), resultJson, 0).Err()
			if err != nil {
				panic(err)
			}

			// Complete the job unless we're 'unlucky' and crash again
			if sharedJobCounter%29 != 0 {
				fmt.Println("Completing")
				sharedQueue.Complete(ctx, db, job)
			} else {
				fmt.Println("Dropping")
			}
		} else {
			goJobCounter++

			// First, try to get a job from the go job queue
			block := sharedJobCounter%6 == 0
			fmt.Println("Leasing go with block =", block)
			job, err := goQueue.Lease(ctx, db, block, 2*time.Second, time.Second)
			if err != nil {
				panic(err)
			}

			// If there was no job, continue.
			// Also, if we get 'unlucky', crash while completing the job.
			if job == nil || goJobCounter%7 == 0 {
				fmt.Println("Dropping job:", job)
				continue
			}

			// Check the data is a single byte
			if len(job.Data) != 1 {
				panic("job data not length 1")
			}
			// Generate the response
			result := []byte{job.Data[0] * 5}
			fmt.Println("Result:", result)
			// Pretend it takes us a while to compute the result
			// Sometimes this will take too long and we'll timeout
			if goJobCounter%25 == 0 {
				time.Sleep(time.Duration(goJobCounter%20) * time.Second)
			}

			// Store the result
			err = db.Set(ctx, goResultsKey.Of(job.ID), result, 0).Err()
			if err != nil {
				panic(err)
			}

			// Complete the job unless we're 'unlucky' and crash again
			if goJobCounter%29 != 0 {
				fmt.Println("Completing")
				completed, err := goQueue.Complete(ctx, db, job)
				if err != nil {
					panic(err)
				}
				if completed {
					fmt.Println("Spawning shared jobs")
					// If we succesfully completed the result, create two new shared jobs.
					item, err := workqueue.NewItemFromJSONData(SharedJobData{
						A: 7,
						B: int(job.Data[0]),
					})
					if err != nil {
						panic(err)
					}
					err = sharedQueue.AddItem(ctx, db, item)
					if err != nil {
						panic(err)
					}

					item, err = workqueue.NewItemFromJSONData(SharedJobData{
						A: 11,
						B: int(job.Data[0]),
					})
					if err != nil {
						panic(err)
					}
					err = sharedQueue.AddItem(ctx, db, item)
					if err != nil {
						panic(err)
					}
				}
			} else {
				fmt.Println("Dropping")
			}
		}
	}
}

func sliceContainsString(slice []string, target string) bool {
	for _, s := range slice {
		if s == target {
			return true
		}
	}
	return false
}
