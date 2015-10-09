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
	log "github.com/Sirupsen/logrus"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	testFileName   string = "test.json.gz"
	testFileNumber int    = 10
	testLineNumber int    = 10000
)

type testData struct {
	Query string
}
type badData struct {
	Query int
}

var filenames []string

func init() {
	for i := 0; i < testFileNumber; i++ {
		fn := fmt.Sprintf("%v%v", i, testFileName)
		f := CreateGZ(fn)
		filenames = append(filenames, fn)
		// generate test data
		for i := 0; i < testLineNumber; i++ {
			var m testData
			switch i {
			case 1:
				var j badData
				if i == 1 {
					j = badData{1}
					b, err := json.Marshal(j)
					if err != nil {
						panic(err)
					}
					WriteGZ(f, string(b))
					WriteGZ(f, "\n")
				}
			case 4:
				m = testData{"unicorn"}
				b, err := json.Marshal(m)
				if err != nil {
					panic(err)
				}
				WriteGZ(f, string(b))
				WriteGZ(f, "\n")
			default:
				m = testData{"test"}
				b, err := json.Marshal(m)
				if err != nil {
					panic(err)
				}
				WriteGZ(f, string(b))
				WriteGZ(f, "\n")
			}
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

func TestMapReduce(t *testing.T) {
	pathsCH := WalkFiles(".", ".gz")
	strings := make(chan string, 100)
	done := make(chan struct{})
	n := 2
	var wg sync.WaitGroup
	wg.Add(n)
	var wg2 sync.WaitGroup
	wg2.Add(n)
	log.Debugf("Starting...")
	for i := 0; i < n; i++ {
		go func() {
			Reduce(strings, done)
			wg2.Done()
		}()
	}
	for i := 0; i < n; i++ {
		go func() {
			Map(pathsCH, strings)
			wg.Done()
		}()
	}
	wg.Wait()
	close(done)
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
