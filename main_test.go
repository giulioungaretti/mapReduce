//
// main_test.go
// Copyright (C) 2015 giulio <giulioungaretti@me.com>
//
// Distributed under terms of the MIT license.
//

package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	testFileName   string = "test.json.gz"
	testFileNumber int    = 10
	testLineNumber int    = 10000
)

type testdata struct {
	Query string
}

var filenames []string

func init() {
	for i := 0; i < testFileNumber; i++ {
		fn := fmt.Sprintf("%v%v", i, testFileName)
		f := CreateGZ(fn)
		filenames = append(filenames, fn)
		// generate test data
		for i := 0; i < testLineNumber; i++ {
			var m testdata
			if i == 4 {
				m = testdata{"unicorn"}
			} else {
				m = testdata{"test"}
			}
			b, err := json.Marshal(m)
			if err != nil {
				panic(err)
			}
			WriteGZ(f, string(b))
			WriteGZ(f, "\n")
		}
		CloseGZ(f)
	}
}

type F struct {
	f  *os.File
	gf *gzip.Writer
	fw *bufio.Writer
}

func CreateGZ(s string) (f F) {

	fi, err := os.OpenFile(s, os.O_WRONLY|os.O_CREATE, 0660)
	if err != nil {
		log.Printf("Error in Create\n")
		panic(err)
	}
	gf := gzip.NewWriter(fi)
	fw := bufio.NewWriter(gf)
	f = F{fi, gf, fw}
	return
}

func WriteGZ(f F, s string) {
	(f.fw).WriteString(s)
}

func CloseGZ(f F) {
	f.fw.Flush()
	// Close the gzip first.
	f.gf.Close()
	f.f.Close()
}

func consume(strings chan string) {
	for _ = range strings {
	}
}

func TestMap(t *testing.T) {
	runtime.GOMAXPROCS(20)
	pathsCH := WalkFiles(".", ".gz")
	strings := make(chan string, 100)
	var wg sync.WaitGroup
	wg.Add(numDigesters)
	go consume(strings)
	for i := 0; i < numDigesters; i++ {
		go func() {
			Map(pathsCH, strings)
			wg.Done()
		}()
	}
	wg.Wait()
	val := atomic.LoadInt64(&mapped)
	if int(val) != testLineNumber*testFileNumber {
		t.Errorf("got %v, expectext %v", val, testLineNumber*testFileNumber)
	}
}

func TestReduce(t *testing.T) {
	runtime.GOMAXPROCS(20)
	pathsCH := WalkFiles(".", ".gz")
	strings := make(chan string, 100)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(numDigesters)
	var wg2 sync.WaitGroup
	wg2.Add(numDigesters)
	for i := 0; i < numDigesters; i++ {
		go func() {
			Reduce(strings, done)
			wg2.Done()
		}()
	}
	for i := 0; i < numDigesters; i++ {
		go func() {
			Map(pathsCH, strings)
			wg.Done()
		}()
	}
	wg.Wait()
	close(done)
	//
	wg2.Wait()
	val := atomic.LoadInt64(&reduced)
	if int(val) != testLineNumber*testFileNumber {
		t.Errorf("got %v, expectext %v", val, testLineNumber*testFileNumber)
	}
	for _, fn := range filenames {
		err := os.Remove(fn)
		if err != nil {
			panic(err)
		}
	}
}
