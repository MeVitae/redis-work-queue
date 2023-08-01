package main

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type Worker struct {
	NewWorkers `yaml:"NewWorkers"`
	ScalerSim  `yaml:"ScalerSim"`
}

type NewWorkers struct {
	AppsV1Deployments string `yaml:"appsV1Deployments"`
	Name              string `yaml:"name"`
	Db                string `yaml:"db"`
	ObjectQueue       string `yaml:"objectQueue"`
	Processing        string `yaml:"processing"`
	CalculatorY       `yaml:"Calculator"`
	Value             int `yaml:"value"`
}
type WorkerY struct {
	NewWorkers `yaml:"NewWorkers"`
	ScalerSim  `yaml:"ScalerSim"`
}

type MainStruct map[string]Worker

type CalculatorY struct {
	Target     int `yaml:"Target"`
	SpotTarget int `yaml:"SpotTarget"`
	Run        int `yaml:"Run"`
	Spinup     int `yaml:"Spinup"`
}

type ScalerSim struct {
	PowerNeededMin []int  `yaml:"PowerNeededMin"`
	NextStage      string `yaml:"NextStage"`
	NumberToSpawn  int    `yaml:"numberToSpawn"`
}

func readYaml() MainStruct {
	data, _ := ioutil.ReadFile("config.yaml")

	var result MainStruct

	err := yaml.Unmarshal([]byte(data), &result)
	if err != nil {
		log.Fatalln(err)
	}

	return result
}
