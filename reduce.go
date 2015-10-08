// reduce.go
// Copyright (C) 2015 giulio <giulioungaretti@me.com>
//
// Distributed under terms of the MIT license.
//

package main

import "sync/atomic"

// Find files in path and counts the values specified in data strcut.
func Reduce(strings chan string, done <-chan struct{}) {
	for {
		select {
		case s := <-strings:
			atomic.AddInt64(&reduced, 1)
			counter.Lock()
			if _, ok := counter.m[s]; ok {
				counter.m[s]++
			} else {
				counter.m[s] = 1
			}
			counter.Unlock()
		case <-done:
			return
		default:
			continue
		}

	}
}
