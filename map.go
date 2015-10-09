// map.go
// Copyright (C) 2015 giulio <giulioungaretti@me.com>
//
// Distributed under terms of the MIT license.
//

package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"os"
	"sync/atomic"

	log "github.com/Sirupsen/logrus"
)

type Data struct {
	Key MaybeString `json:"query"`
}

// Maybestring is a strcture that holds a value that **must** be a string
// but may be a bool or number   in the original data
type MaybeString struct {
	Value string
}

// UnmarshalJSON teaches MaybeString how to parse itself
func (a *MaybeString) UnmarshalJSON(b []byte) (err error) {
	s := ""
	// unmarshall string into string
	// which gives nil error if the
	// input byte stream is indeed a string
	if err = json.Unmarshal(b, &s); err == nil {
		a.Value = s
	} else {
		// hack to handle special cases
		if string(b) == "false" {
			// YOLO
			a.Value = ""
			err = nil
			// if input is a int
			// unmarhsall will fail
			// so cast to string
		} else {
			// NOTE this may be a bug in api
			a.Value = string(b)
			err = nil
		}
	}
	return err
}

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
			stringsCH <- v.Key.Value
		}
		atomic.AddInt64(&files, 1)
		log.Debugf("done with %v", path)
	}
}
