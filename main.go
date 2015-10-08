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
)

const (
	numDigesters = 1
)

var (
	mapped  int64
	reduced int64
	files   int64
)

var counter = struct {
	sync.RWMutex
	m map[string]int
}{m: make(map[string]int)}

type Data struct {
	Key string `json:"query"`
}

func PrintStatus() {
	val := atomic.LoadInt64(&mapped)
	valbad := atomic.LoadInt64(&reduced)
	filesc := atomic.LoadInt64(&files)
	fmt.Printf("files:mapped:reduced %v, %v:%v \n", filesc, val, valbad)
}

func main() {
	runtime.GOMAXPROCS(20)
	var path = flag.String("path", ".", "root path of the data files.")
	var out = flag.String("out", ".", " path of the output csv file.")
	flag.Parse()
	pathsCH := WalkFiles(*path, ".gz")
	strings := make(chan string)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numDigesters)
	var wg2 sync.WaitGroup
	wg2.Add(numDigesters)
	for i := 0; i < numDigesters; i++ {
		go func(done chan struct{}) {
			Reduce(strings, done)
			wg2.Done()
		}(done)
	}
	for i := 0; i < numDigesters; i++ {
		for p := range pathsCH {
			go func() {
				Map(p, strings)
				wg.Done()
			}()
		}
	}
	wg.Wait()
	close(done)
	wg2.Wait()
	//csvfile, err := os.Create(*out)
	//if err != nil {
	//fmt.Println("Error:", err)
	//return
	//}
	//defer csvfile.Close()
	f, err := os.OpenFile(*out, os.O_WRONLY, 0644)
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
