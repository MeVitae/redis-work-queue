package main

import (
	"context"
	"fmt"
	"go-auto-scaller-simulator/autoScallerSim"
	"go-auto-scaller-simulator/graphs"
	"os"
	"time"

	autoscaller "github.com/mevitae/redis-work-queue/autoscale/wqautoscale"
	"gopkg.in/yaml.v2"
)

func LoadConfig(configPath string) (autoscaller.Config, error) {
	var config autoscaller.Config
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

func main() {
	WorkersConfig := autoScallerSim.GetWorkersConfig("workers.yaml")
	deployments := make(autoScallerSim.SimulatedDeployments)

	config := autoScallerSim.GetConfig("config.yaml")
	tickChan := make(chan *autoScallerSim.Workers, 5)
	graphChan := make(chan graphs.GraphInfo)
	deploymentGraphs := make(map[string]*graphs.ChartData)

	tick := 0
	//NChart := graphs.StartPlotGraph(deployment.podType)
	for index, _ := range config.MainStruct {
		//fmt.Println(index)
		(deploymentGraphs)[index] = graphs.StartPlotGraph(index)
	}

	//go graphs.manageGraphChan(&deploymentGraphs, &graphChan)
	go manageGraphChan(&deploymentGraphs, &graphChan, &tick)
	go autoScallerSim.Start(&tick, &tickChan, config.MainStruct, WorkersConfig, &graphChan, config.ProcessStarterConfig, deployments)

	Config, err := LoadConfig("nConfig.yaml")
	if err != nil {
		panic(err)
	}
	fmt.Println("Running", Config)
	startTime := time.Now()

	time.Sleep(time.Second)

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Minute)
	autoScaler, err := autoscaller.NewAutoScale(ctx, &deployments, &deployments, Config)

	for elem := range tickChan {

		timeOffset := time.Now().Add(100 * time.Millisecond * time.Duration(tick)).Sub(startTime)
		ctx, _ = context.WithTimeout(context.Background(), 2*time.Minute)
		err = autoScaler.Scale(ctx, timeOffset.Milliseconds())
		if err != nil {
			panic(err)
		}
		elem.ProcessWorkersChange()
		for index, _ := range config.MainStruct {
			deploymentGraphs[index].AddGraphData("Jobs", float32(deployments[index].Deployment.QueueLen()))
			deploymentGraphs[index].AddGraphData("Tick", float32(tick))
			readyWorkers, workers := deployments[index].Deployment.GetWorkers()

			deploymentGraphs[index].AddGraphData("Workers", float32(workers))
			deploymentGraphs[index].AddGraphData("ReadyWorkers", float32(readyWorkers))
		}

		//autoScaller.workers = elem
		//deploymentGraphs[elem.DepName].TotalSeconds = append(deploymentGraphs[elem.DepName].TotalSeconds, int32(tick))
		elem.Verify <- true
	}
}

func manageGraphChan(graph *map[string]*graphs.ChartData, graphData *chan graphs.GraphInfo, tick *int) {

	for elem := range *graphData {
		(*graph)[elem.Deployment].AddGraphData(elem.DataType, elem.Data)
	}
}
