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
	autoscale, err := autoscale.InClusterAutoscaler(ctx, configPath, 0)
	cancel()
	if err != nil {
		panic(err)
	}
	for {
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
