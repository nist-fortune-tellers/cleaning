package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

const outputCompleteData = false

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

	log.Println("Writing File")
	writeFile(ids, dps, outputFile)
	log.Println("Done!")
}

type laneID int
type dataMap map[laneID]*dataPoint

//Lane is an abstract representation of a single datapoint in the file.
type dataPoint struct {
	splits []string
}

//NewLane makes a new init'd lane object from a line.
func newDataPoint(line string) (laneID, *dataPoint) {
	//Example Format:
	//Mon Jan 2 15:04:05 -0700 MST 2006
	//2011-05-01 00:17
	const timeFormat = "2006-01-02 15:04"

	splits := strings.Split(line, "\t")
	//Example Line:
	// 10056	2011-05-01 00:17	0	30	2
	laneIDint, err := strconv.Atoi(splits[0])
	check(err)

	return laneID(laneIDint), &dataPoint{splits}
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

func writeFile(ids []laneID, dps dataMap, output string) {
	file, err := os.Create(output)
	check(err)
	defer file.Close()
	bw := bufio.NewWriter(file)
	defer bw.Flush()
	for _, id := range ids {
		dp := dps[id]
		dp.writeLines(bw)
		bw.WriteString("\n")
	}
}

func scanFile(numLines int, fileToScan string) dataMap {
	//create a slice with the number of lines needed.
	dps := make(dataMap, int(float64(numLines)*1.1))
	//open the file for reading.
	file, err := os.Open(fileToScan)
	check(err)
	defer file.Close()

	//Start scanning line-by-line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, dp := newDataPoint(scanner.Text())
		dps[key] = dp
	}
	return dps
}

func getIDList(input string) []laneID {
	numLines, err := lineCounter(input)
	check(err)
	fmt.Println("Number of Lines in Original Input File: ", numLines)
	ids := make([]laneID, 0, numLines)
	//open the file for reading.
	file, err := os.Open(input)
	check(err)
	defer file.Close()

	//Start scanning line-by-line
	scanner := bufio.NewScanner(file)
	//skip first line
	scanner.Scan()
	fmt.Println("Retrieving List of Indexes from ", input)
	for scanner.Scan() {
		line := scanner.Text()
		index := strings.Index(line, ",")
		id, err := strconv.Atoi(line[:index])
		check(err)
		ids = append(ids, laneID(id))
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
