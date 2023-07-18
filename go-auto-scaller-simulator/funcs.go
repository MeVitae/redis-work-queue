package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
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
func generateRandomFloat64(min, max float64) float64 {
	rand.Seed(time.Now().UnixNano())
	result := min + rand.Float64()*(max-min)
	return result
}

func getMeanTime(numbers []int) float64 {
	sum := 0
	count := len(numbers)

	if count == 0 {
		return 0
	}

	for _, num := range numbers {
		sum += num
	}

	mean := float64(sum) / float64(count)
	return mean
}
func writeToFile(fileName string, toWrite string) {
	filePath := "./savedData/" + fileName + ".txt"
	content := toWrite + "\n"

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		file, err := os.Create(filePath)
		if err != nil {
			fmt.Println("Error creating file:", err)
			return
		}
		defer file.Close()
	}

	// Read the existing content from the file
	existingContent, err := ioutil.ReadFile(filePath)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	// Combine the new line and existing content
	newContent := strings.Join([]string{string(existingContent), content}, "")

	// Write the new content back to the file
	err = ioutil.WriteFile(filePath, []byte(newContent), 0644)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}
}
