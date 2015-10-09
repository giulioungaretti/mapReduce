// map.go
// Copyright (C) 2015 giulio <giulioungaretti@me.com>
//
// Distributed under terms of the MIT license.
//

package main

import (
	"bufio"
	"compress/gzip"
	"os"
	"strconv"
	"sync/atomic"

	log "github.com/Sirupsen/logrus"
)

// Find files in path and counts the values specified in data
func Map(pathsCH <-chan string, stringsCH chan string) {
	for path := range pathsCH {
		log.Debugf("start with %v", path)
		file, err := os.Open(path)
		if err != nil {
			log.Errorf("%v", err)
		}
		gzfile, err := gzip.NewReader(file)
		if err != nil {
			log.Errorf("%v", err)
		}
		scanner := bufio.NewScanner(gzfile)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			atomic.AddInt64(&mapped, 1)
			b := scanner.Bytes()
			line := make([]byte, len(b))
			copy(line, b)
			v, err := UMarsh(line)
			if err != nil {
				log.Errorf("%v :%v ", err, string(line))
				continue
			}
			switch t := v.Key.(type) {
			case string:
				stringsCH <- t
			case int:
				stringsCH <- strconv.Itoa(t)
			}
		}
		atomic.AddInt64(&files, 1)
		log.Debugf("done with %v", path)
	}
}
