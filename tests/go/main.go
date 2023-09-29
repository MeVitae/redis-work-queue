package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	workqueue "github.com/mevitae/redis-work-queue/go"
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
	db := redis.NewClient(&redis.Options{
		Addr: os.Args[1],
	})

	src := rand.NewSource(time.Now().UnixNano())
	random := rand.New(src)

	ctx := context.Background()

	goResultsKey := workqueue.KeyPrefix("results:go:")
	sharedResultsKey := workqueue.KeyPrefix("results:shared:")

	goQueue := workqueue.NewWorkQueue(workqueue.KeyPrefix("go_jobs"))
	sharedQueue := workqueue.NewWorkQueue(workqueue.KeyPrefix("shared_jobs"))
	goJobCounter := 0
	sharedJobCounter := 0

	shared := false
	for {
		shared = !shared
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
				//time.Sleep(time.Duration(goJobCounter%20) * time.Second)
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
					if random.Intn(10) < 5 {
						for i := 0; i < 3; i++ {
							go func() {
								for i := 0; i < 10; i++ {
									_, err = sharedQueue.AddNewItem(ctx, db, item)
									if err != nil {
										fmt.Println(err)
									}
								}
							}()
						}
					} else {
						for i := 0; i < 3; i++ {
							go func() {
								for i := 0; i < 10; i++ {
									_, err = AddNewItemWithSleep(ctx, db, item, &sharedQueue, workqueue.KeyPrefix("shared_jobs").Of(":processing"), workqueue.KeyPrefix("shared_jobs").Of(":queue"))
									if err != nil {
										fmt.Println(err)
									}
								}
							}()
						}
					}
					item2, err := workqueue.NewItemFromJSONData(SharedJobData{
						A: 11,
						B: int(job.Data[0]),
					})
					if err != nil {
						panic(err)
					}
					if random.Intn(10) < 5 {
						for i := 0; i < 3; i++ {
							go func() {
								for i := 0; i < 10; i++ {
									_, err = sharedQueue.AddNewItem(ctx, db, item2)
									if err != nil {
										fmt.Println(err)
									}
								}
							}()
						}
					} else {
						for i := 0; i < 3; i++ {
							go func() {
								for i := 0; i < 10; i++ {
									_, err = AddNewItemWithSleep(ctx, db, item2, &sharedQueue, workqueue.KeyPrefix("shared_jobs").Of(":processing"), workqueue.KeyPrefix("shared_jobs").Of(":queue"))
									if err != nil {
										fmt.Println(err)
									}
								}
							}()
						}
					}
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

func AddNewItemWithSleep(ctx context.Context, db *redis.Client, item workqueue.Item, workQueue *workqueue.WorkQueue, mainKey, processingKey string) (bool, error) {
	txf := func(tx *redis.Tx) error {

		processingItemsInQueueCmd := tx.LPos(ctx, processingKey, item.ID, redis.LPosArgs{
			Rank:   0,
			MaxLen: 0,
		})
		workingItemsInQueueCmd := tx.LPos(ctx, mainKey, item.ID, redis.LPosArgs{
			Rank:   0,
			MaxLen: 0,
		})

		_, ProcessingQueueCheck := processingItemsInQueueCmd.Result()
		_, WorkingQueueCheck := workingItemsInQueueCmd.Result()

		if ProcessingQueueCheck == redis.Nil && WorkingQueueCheck == redis.Nil {
			time.Sleep(100 * time.Millisecond)
			_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				workQueue.AddItemToPipeline(ctx, pipe, item)
				_, err := pipe.Exec(ctx)
				return err
			})
			return err
		} else {
			return nil
		}
	}

	for i := 0; i < 100; i++ {
		err := db.Watch(ctx, txf, processingKey, mainKey)
		if err == nil {
			return true, nil
		}
		if err == redis.TxFailedErr {
			continue
		}
		return false, err
	}

	return false, errors.New("increment reached maximum number of retries")
}
