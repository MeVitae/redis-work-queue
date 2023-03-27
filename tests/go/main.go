package main

import (
	"context"
	"encoding/json"
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
			job, err := sharedQueue.Lease(ctx, db, true, 2*time.Second, 4*time.Second)
			if err != nil {
				panic(err)
			}
			// If there was no job, continue.
			// Also, if we get 'unlucky', crash while completing the job.
			if job == nil || sharedJobCounter%7 == 0 {
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
			// Pretend it takes us a while to compute the result
            // Sometimes this will take too long and we'll timeout
			time.Sleep(time.Second * time.Duration(sharedJobCounter % 7))

            // Store the result
			err = db.Set(ctx, sharedResultsKey.Of(job.ID), resultJson, 0).Err()
			if err != nil {
				panic(err)
			}

            // Complete the job unless we're 'unlucky' and crash again
			if sharedJobCounter%29 != 0 {
				sharedQueue.Complete(ctx, db, job)
			}
		} else {
			goJobCounter++

			// First, try to get a job from the go job queue
			job, err := goQueue.Lease(ctx, db, true, 2*time.Second, 1*time.Second)
			if err != nil {
				panic(err)
			}

			// If there was no job, continue.
			// Also, if we get 'unlucky', crash while completing the job.
			if job == nil || goJobCounter%7 == 0 {
				continue
			}

			// Check the data is a single byte
			if len(job.Data) != 1 {
				panic("job data not length 1")
			}
			// Generate the response
			result := []byte{job.Data[0] * 5}
			// Pretend it takes us a while to compute the result
            // Sometimes this will take too long and we'll timeout
            if goJobCounter % 11 == 0 {
                time.Sleep(time.Duration(goJobCounter%20)*time.Second)
            }

            // Store the result
			err = db.Set(ctx, goResultsKey.Of(job.ID), result, 0).Err()
			if err != nil {
				panic(err)
			}

            // Complete the job unless we're 'unlucky' and crash again
			if goJobCounter%29 != 0 {
				completed, err := goQueue.Complete(ctx, db, job)
				if err != nil {
					panic(err)
				}
				if completed {
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
			}
		}
	}
}
