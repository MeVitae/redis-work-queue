package autoScallerSim

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/go-echarts/go-echarts/v2/opts"
)

var filePath = "./queue-times2.txt"

type jobTimings struct {
	tickTime map[string]string
}

func getTickJobTimings(mod bool) jobTimings {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	RealtikTimingJobs := jobTimings{
		tickTime: make(map[string]string),
	}
	if mod {
		currentTick := 0
		tikTimingJobs := jobTimings{
			tickTime: make(map[string]string),
		}

		for scanner.Scan() {
			line := scanner.Text()
			linenr, err := strconv.Atoi(line)
			if err != nil {
				panic(err)
			}
			tikTimingJobs.tickTime[strconv.Itoa(linenr)] = "true"
		}

		for i := 0; i < 10000; i++ {
			num1 := generateRandomNumber(0, 12000)
			num2 := generateRandomNumber(num1, 12000)
			for i := num1; i < num2; i++ {
				_, exists := tikTimingJobs.tickTime[strconv.Itoa(i)]
				if exists {
					RealtikTimingJobs.tickTime[strconv.Itoa(currentTick)] = "true"
				}
				currentTick++
			}
		}
		fmt.Println(currentTick)

		if scanner.Err() != nil {
			log.Fatalf("Error while scanning file: %v", scanner.Err())
		}
		return RealtikTimingJobs
	} else {
		biggest := 0
		for scanner.Scan() {
			line := scanner.Text()
			linesp := strings.Split(line, " ")
			linenr, err := strconv.Atoi(linesp[0])
			if err != nil {
				panic(err)
			}
			biggest = linenr
			RealtikTimingJobs.tickTime[strconv.Itoa(linenr)] = linesp[1]
		}
		for i, data := range RealtikTimingJobs.tickTime {
			if RealtikTimingJobs.tickTime[i] != "" {
				num, _ := strconv.Atoi(i)
				RealtikTimingJobs.tickTime[strconv.Itoa(num+biggest)] = data

			}
		}
		if scanner.Err() != nil {
			log.Fatalf("Error while scanning file: %v", scanner.Err())
		}
		return RealtikTimingJobs
	}
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
