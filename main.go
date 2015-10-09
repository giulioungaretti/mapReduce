//maing.go
// Copyright (C) 2015 giulio <giulioungaretti@me.com>
//
// Distributed under terms of the MIT license.
//
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
)

var (
	mapped       int64
	reduced      int64
	files        int64
	numDigesters int
	numFiles     int
)

var counter = struct {
	sync.RWMutex
	m map[string]int
}{m: make(map[string]int)}

func PrintStatus() {
	val := atomic.LoadInt64(&mapped)
	valbad := atomic.LoadInt64(&reduced)
	filesc := atomic.LoadInt64(&files)
	fmt.Printf("files:mapped:reduced %v:%v:%v \n", filesc, val, valbad)
}

func main() {
	// Only log the warning severity or above.
	//log.SetLevel(log.DebugLevel)
	runtime.GOMAXPROCS(runtime.NumCPU() + 1)
	//
	var path = flag.String("path", ".", "root path of the data files.")
	var out = flag.String("out", ".", " path of the output csv file.")
	numDigesters = *flag.Int("numDigesters", runtime.NumCPU()-1, "number of digesters to run in parallel")
	numFiles = *flag.Int("numFiles", runtime.NumCPU()+1, "number of files to read in parallel")
	flag.Parse()
	//
	// printing status every 10 sec
	//
	go func() {
		for {
			select {
			case <-time.After(10 * time.Second):
				PrintStatus()
			}
		}
	}()
	//
	//  map reduce
	//
	pathsCH := WalkFiles(*path, ".gz")
	strings := make(chan string, 100)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numFiles)
	var wg2 sync.WaitGroup
	wg2.Add(numDigesters)
	log.Debugf("Starting...")
	for i := 0; i < numDigesters; i++ {
		go func() {
			Reduce(strings, done)
			wg2.Done()
		}()
	}
	for i := 0; i < numFiles; i++ {
		go func() {
			Map(pathsCH, strings)
			wg.Done()
		}()
	}
	wg.Wait()
	close(done)
	wg2.Wait()
	//
	// csv out
	//
	log.Debugf("Writing CSV to disk")
	f, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE, 0660)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	writer := csv.NewWriter(f)
	for value, count := range counter.m {
		err := writer.Write([]string{value, strconv.Itoa(count)})
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
	}
	writer.Flush()
	PrintStatus()
}
