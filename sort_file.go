package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const outputCompleteData = true

//Example Format:
//Mon Jan 2 15:04:05 -0700 MST 2006
//2011-05-01 00:17
const timeFormat = "2006-01-02 15:04"

//Lane is an abstract representation of a single datapoint in the file.
type dataPoint struct {
	line     string
	laneID   int
	unixTime int64
}

type dataPoints []dataPoint

func (dps dataPoints) Len() int {
	return len(dps)
}

func (dps dataPoints) Swap(i, j int) {
	dps[i], dps[j] = dps[j], dps[i]
}

type byLane struct{ dataPoints }

func (bl byLane) Less(i, j int) bool {
	return bl.dataPoints[i].laneID < bl.dataPoints[j].laneID
}

type byDate struct{ dataPoints }

func (bl byDate) Less(i, j int) bool {
	return bl.dataPoints[i].unixTime < bl.dataPoints[j].unixTime
}

//NewLane makes a new init'd lane object from a line.
func newDataPoint(rawLine string) dataPoint {
	splits := strings.Split(rawLine, "\t")
	//Example Line:
	// 10056	2011-05-01 00:17	0	30	2
	laneID, err := strconv.Atoi(splits[0])
	check(err)
	time, err := time.Parse(timeFormat, splits[1])
	check(err)

	//make line just the necessary ending part.
	var line string
	if outputCompleteData {
		line = rawLine
	} else if len(splits) == 4 {
		line = fmt.Sprintf("%v\t%v\n", splits[2], splits[3])
	} else if len(splits) == 5 {
		line = fmt.Sprintf("%v\t%v\t%v\n", splits[2], splits[3], splits[4])
	}

	return dataPoint{
		line:     line,
		laneID:   laneID,
		unixTime: time.Unix(),
	}
}

func main() {
	fileToSort := os.Args[1]
	outputFile := os.Args[2]

	numLines, err := lineCounter(fileToSort)
	check(err)
	log.Println("The Number Of Lines Is: ", numLines)
	log.Println("Scanning ", fileToSort, " Into Memory")
	dps := scanFile(numLines, fileToSort)
	log.Println("Sorting By Lane")
	sort.Sort(byLane{dps})
	log.Println("Sorting By Date")
	sort.Sort(byDate{dps})
	log.Println("Writing File")
	writeFile(dps, outputFile)
	log.Println("Done!")
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func writeFile(dps dataPoints, output string) {
	file, err := os.Create(output)
	check(err)
	defer file.Close()
	bw := bufio.NewWriter(file)
	defer bw.Flush()
	for _, dp := range dps {
		bw.WriteString(dp.line)
	}
}

func scanFile(numLines int, fileToScan string) dataPoints {
	//create a slice with the number of lines needed.
	dps := make(dataPoints, 0, numLines)
	//open the file for reading.
	file, err := os.Open(fileToScan)
	check(err)
	defer file.Close()

	//Start scanning line-by-line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		dps = append(dps, newDataPoint(scanner.Text()))
	}
	return dps
}

func lineCounter(fileToSort string) (int, error) {
	r, err := os.Open(fileToSort)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	buf := make([]byte, 8196)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}

		count += bytes.Count(buf[:c], lineSep)

		if err == io.EOF {
			break
		}
	}

	return count, nil
}
