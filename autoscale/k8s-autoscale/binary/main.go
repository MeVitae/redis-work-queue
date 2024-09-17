package main

import (
	"context"
	"fmt"
	"os"
	"time"

	autoscale "github.com/mevitae/redis-work-queue/autoscale/k8s-autoscale"
)

func main() {
	configPath := "config.yaml"
	switch len(os.Args) {
	case 0, 1:
		fmt.Fprintln(os.Stderr, "No config file specified, defaulting to", configPath)
		break
	case 2:
		configPath = os.Args[1]
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	startTime := time.Now()
	autoscale, cleaner, err := autoscale.InClusterAutoscaler(ctx, configPath, 0, nil)
	cancel()
	if err != nil {
		panic(err)
	}
	for cleanCounter := uint8(1); ; cleanCounter = (cleanCounter + 1) % 72 {
		if cleanCounter == 0 {
			fmt.Println("======================================================================")
			for _, queue := range autoscale.QueueNames() {
				fmt.Println("Deep cleaning", queue)
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
				err = cleaner.DeepClean(ctx, queue)
				if err != nil {
					panic(err)
				}
				cancel()
			}
		} else if cleanCounter%3 == 0 {
			fmt.Println("======================================================================")
			for _, queue := range autoscale.QueueNames() {
				fmt.Println("Light cleaning", queue)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				err = cleaner.LightClean(ctx, queue)
				if err != nil {
					panic(err)
				}
				cancel()
			}
		}

		timeOffset := time.Since(startTime)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		fmt.Println("======================================================================")
		err = autoscale.Scale(ctx, timeOffset.Milliseconds())
		if err != nil {
			panic(err)
		}
		cancel()
		time.Sleep(time.Second * 10)
	}
}
