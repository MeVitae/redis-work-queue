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
