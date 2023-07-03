package main

import (
	"math/rand"
	"time"
)

func generateRandomNumber(min, max int) int {
	rand.Seed(time.Now().UnixNano())

	return min + rand.Intn(max-min+1)
}
func generateRandomFloat(min, max float32) float32 {
	rand.Seed(time.Now().UnixNano())
	result := min + rand.Float32()*(max-min)
	return result
}
