package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/go-echarts/go-echarts/v2/opts"
)

var filePath = "./queue-times.txt"

type jobTimings struct {
	tickTime map[string]bool
}

func getTickJobTimings() jobTimings {
	currentTick := 0
	RealtikTimingJobs := jobTimings{
		tickTime: make(map[string]bool),
	}

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	tikTimingJobs := jobTimings{
		tickTime: make(map[string]bool),
	}

	for scanner.Scan() {
		line := scanner.Text()
		tikTimingJobs.tickTime[line] = true
	}

	for i := 0; i < 1000; i++ {
		num1 := generateRandomNumber(0, 12000)
		//fmt.Println(num1)
		num2 := generateRandomNumber(num1, 12000)
		//fmt.Println(num2)
		for i := num1; i < num2; i++ {
			currentTick++
			_, exists := tikTimingJobs.tickTime[strconv.Itoa(i)]
			//fmt.Println(exists)
			if exists {
				//fmt.Println(value, "=")
				RealtikTimingJobs.tickTime[strconv.Itoa(currentTick)] = true
			}
		}
	}
	fmt.Println(currentTick)

	if scanner.Err() != nil {
		log.Fatalf("Error while scanning file: %v", scanner.Err())
	}

	return RealtikTimingJobs
}

func GetFileDataFromSavedData(fileName string) []opts.LineData {
	filePath := "./savedData/" + fileName + ".txt"
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var data []opts.LineData // Create a new slice to store the converted values

	for scanner.Scan() {
		line := scanner.Text()

		// Convert the line to an integer or float64
		value, err := strconv.ParseFloat(line, 64) // Use strconv.ParseFloat(line, 64) for float64
		if err != nil {
			log.Fatalf("Failed to convert line to integer: %v", err)
		}

		// Create an instance of opts.LineData and set the fields
		lineData := opts.LineData{
			Name:  line,
			Value: value,
		}

		data = append(data, lineData) // Add the converted value to the slice
	}

	if scanner.Err() != nil {
		log.Fatalf("Error while scanning file: %v", scanner.Err())
	}

	return data
}
