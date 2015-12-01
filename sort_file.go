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

func main() {
	args := os.Args[len(os.Args)-3:]
	originalFile := args[0]
	fileToSort := args[1]
	outputFile := args[2]

	fmt.Println("Processing Original File...")
	ids := getIDList(originalFile)

	numLines, err := lineCounter(fileToSort)
	check(err)
	log.Println("The Number Of Lines Is: ", numLines)
	log.Println("Scanning ", fileToSort, " Into Memory")
	dps := scanFile(numLines, fileToSort)

	log.Println("Sorting...")
	sort.Sort(multiSorter{dps})

	log.Println("Writing File")
	writeFile(dps, outputFile)
	log.Println("Done!")
}

//Lane is an abstract representation of a single datapoint in the file.
type dataPoint struct {
	splits   []string
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

func (dps dataPoints) idLess(i, j int) bool {
	return dps[i].laneID < dps[j].laneID
}

func (dps dataPoints) timeLess(i, j int) bool {
	return dps[i].unixTime < dps[j].unixTime
}

type multiSorter struct{ dataPoints }

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that is either Less or
// !Less. Note that it can call the less functions twice per call. We
// could change the functions to return -1, 0, 1 and reduce the
// number of calls for greater efficiency: an exercise for the reader.
func (ms multiSorter) Less(i, j int) bool {
	switch {
	//first priority is date
	case ms.dataPoints.timeLess(i, j):
		return true
	case ms.dataPoints.timeLess(j, i):
		return false
	}
	//otherwise, just return the last comparison.
	return ms.dataPoints.idLess(i, j)

}

//NewLane makes a new init'd lane object from a line.
func newDataPoint(line string) dataPoint {
	//Example Format:
	//Mon Jan 2 15:04:05 -0700 MST 2006
	//2011-05-01 00:17
	const timeFormat = "2006-01-02 15:04"

	splits := strings.Split(line, "\t")
	//Example Line:
	// 10056	2011-05-01 00:17	0	30	2
	laneID, err := strconv.Atoi(splits[0])
	check(err)
	time, err := time.Parse(timeFormat, splits[1])
	check(err)

	return dataPoint{
		splits:   splits,
		laneID:   laneID,
		unixTime: time.Unix(),
	}
}

func (dp dataPoint) writeLines(w *bufio.Writer) {
	splits := dp.splits
	if outputCompleteData {
		w.WriteString(splits[0])
		w.WriteString("\t")
		w.WriteString(splits[1])
		w.WriteString("\t")
	}
	if len(splits) >= 4 {
		w.WriteString(splits[2])
		w.WriteString("\t")
		w.WriteString(splits[3])
	}
	if len(splits) == 5 {
		w.WriteString("\t")
		w.WriteString(splits[4])
	}
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
		dp.writeLines(bw)
		bw.WriteString("\n")
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

func getIDList(input string) []int {
	numLines, err := lineCounter(input)
	check(err)
	fmt.Println("Number of Lines in Original Input File: ", numLines)
	ids := make([]int, 0, numLines)
	//open the file for reading.
	file, err := os.Open(input)
	check(err)
	defer file.Close()

	//Start scanning line-by-line
	scanner := bufio.NewScanner(file)
	//skip first line
	scanner.Scan()
	fmt.Println("Retrieving List of Indexes")
	for scanner.Scan() {
		line := scanner.Text()
		index := strings.Index(line, ",")
		id, err := strconv.Atoi(line[:index])
		check(err)
		ids = append(ids, id)
	}
	return ids
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
