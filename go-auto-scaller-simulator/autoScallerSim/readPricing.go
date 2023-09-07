package autoScallerSim

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type Configuration struct {
	VCPU        float64                `yaml:"vCPU"`
	VCPUSpot    float64                `yaml:"vCPUSpot"`
	Memory      float64                `yaml:"Memory"`
	MemorySpot  float64                `yaml:"MemorySpot"`
	Storage     float64                `yaml:"Storage"`
	StorageSpot float64                `yaml:"StorageSpot"`
	Workers     map[string]WorkerGroup `yaml:"workers"`
}

type WorkerGroup struct {
	Workers []WorkerResources `yaml:"workers"`
}

type WorkerResources struct {
	WorkerType string  `yaml:"workerType"`
	Type       string  `yaml:"type"`
	VCPu       float64 `yaml:"vCPu"`
	Memory     float64 `yaml:"Memory"`
	Storage    float64 `yaml:"Storage"`
}

func GetPricing(filePath string) (result Configuration) {
	data, _ := ioutil.ReadFile(filePath)
	err := yaml.Unmarshal([]byte(data), &result)
	if err != nil {
		log.Fatalln(err)
	}

	return result
}
