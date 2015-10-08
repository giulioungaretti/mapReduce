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
)

var mapped int64
var reduced int64
var files int64

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
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup

	for p := range pathsCH {
		wg.Add(1)
		go func(strings chan string) {
			Map(p, strings)
			wg.Done()
		}(strings)
	}
	go Reduce(strings, wg2)
	go func() {
		for {
			PrintStatus()
			time.Sleep(10 * time.Second)
		}
	}()
	wg.Wait()
	wg2.Wait()
	//csvfile, err := os.Create(*out)
	//if err != nil {
	//fmt.Println("Error:", err)
	//return
	//}
	//defer csvfile.Close()
	f, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
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
