package autoScallerSim

import (
	"math/rand"
)

func generateRandomBool(a, b int) bool {
	return rand.Intn(b) < a
}

func generateRandomNumber(min, max int) int {
	return min + rand.Intn(max-min+1)
}

func (deployment *deploymentStruct) CalculateCost(pricePerHour float32, spotPrice float32, fastPrice float32) (cost float32) {
	for _, worker := range deployment.workers {
		//fmt.Println(worker.cost)
		if worker.wtype == "spot" {
			cost += (pricePerHour / 10) / spotPrice
		} else if worker.wtype == "fast" {
			cost += (pricePerHour / 10) * fastPrice
		} else {
			cost += pricePerHour / 10
		}
	}
	return
}
