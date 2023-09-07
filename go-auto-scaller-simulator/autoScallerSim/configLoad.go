package autoScallerSim

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type Worker struct {
	CalculatorY `yaml:"Calculator"`
	ScalerSim   `yaml:"ScalerSim"`
}

type MainStruct map[string]Worker

type CalculatorY struct {
	Target     int `yaml:"Target"`
	SpotTarget int `yaml:"SpotTarget"`
	Run        int `yaml:"Run"`
	Spinup     int `yaml:"Spinup"`
}
type ProcessStarter struct {
	StartRange   int    `yaml:"StartRange"`
	StartProcess string `yaml:"StartProcess"`
	Start        string
}
type ProcessStarterConfig []ProcessStarter
type ScalerSim struct {
	PowerNeededMin         []int   `yaml:"PowerNeededMin"`
	ChildWorker            string  `yaml:"ChildWorker"`
	NumberOfChildren       int     `yaml:"NumberOfChildren"`
	PricePerHour           float32 `yaml:"BasePricePerHour"`
	SpotTimesCheaper       float32 `yaml:"SpotTimesCheaper"`
	FastTimesMoreExpensive float32 `yaml:"FastTimesMoreExpensive"`
}
type WorkerCofig struct {
	MainStruct           `yaml:"workers"`
	ProcessStarterConfig `yaml:"expectedInputs"`
}

func GetConfig(filePath string) (result WorkerCofig) {
	data, _ := ioutil.ReadFile(filePath)
	err := yaml.Unmarshal([]byte(data), &result)
	if err != nil {
		log.Fatalln(err)
	}

	return result
}
