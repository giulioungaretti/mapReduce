//
// utils.go
// Copyright (C) 2015 giulio <giulioungaretti@me.com>
//
// Distributed under terms of the MIT license.
//

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// WalkFiles starts a goroutine that send to  channel the paths  starting from root recursively, of files
// whose extension matches ext, NOTE hidden files included.
// Errors are send thorugh the errorc channel, when done is closed, then no more work is done.
func WalkFiles(root string, ext string) <-chan string {
	paths := make(chan string)
	go func() {
		// Close the paths channel after Walk returns.
		defer close(paths)
		// Visit fucntion for walk
		// returns path for file if file extension
		// matches ext. F.ex. ".gz", ".zip"
		var visit = func(path string, f os.FileInfo, err error) error {
			if err != nil {
				panicmsg := fmt.Sprintf("Something wrong happens when walking down the data directory.\n Erorr:%v", err)
				panic(panicmsg)
			}
			if filepath.Ext(path) == ext {
				select {
				case paths <- path:
				}
			}
			// no error returned
			return nil
		}
		// send value through error channel
		filepath.Walk(root, visit)
	}()
	return paths
}

func UMarsh(line []byte) (Data, error) {
	var d Data
	err := json.Unmarshal(line, &d)
	if err != nil {
		return Data{}, err
	}
	return d, nil
}
