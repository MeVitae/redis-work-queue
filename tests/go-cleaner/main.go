package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	workqueue "github.com/mevitae/redis-work-queue/go"
	"github.com/redis/go-redis/v9"
)

func main() {
	if len(os.Args) < 2 {
		panic("first command line argument must be redis host")
	}

	db := redis.NewClient(&redis.Options{
		Addr: os.Args[1],
	})
	ctx := context.Background()

	stdin := bufio.NewReader(os.Stdin)
	for {
		instruction, err := stdin.ReadString('\n')
		if err != nil {
			panic(err)
		}
		instruction = instruction[:len(instruction)-1]
		if queueName, ok := strings.CutPrefix(instruction, "light:"); ok {
			queue := workqueue.NewWorkQueue(workqueue.KeyPrefix(queueName))
			err = queue.LightClean(ctx, db)
			fmt.Println("light cleaned", queueName)
		} else if queueName, ok := strings.CutPrefix(instruction, "deep:"); ok {
			queue := workqueue.NewWorkQueue(workqueue.KeyPrefix(queueName))
			err = queue.DeepClean(ctx, db)
			fmt.Println("deep cleaned", queueName)
		} else {
			panic("invalid cleaning mode")
		}
		if err != nil {
			panic(err)
		}
	}
}
