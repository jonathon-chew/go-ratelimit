package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func Test_Allow(t *testing.T) {
	tb := NewTokenBucket(5, 1)

	for i := range 7 {
		tokens := tb.Tokens
		allowed := tb.Allow()
		if allowed && tokens >= 1 {
			// t.Logf("[INFO]: Successfully allowed a request")
		} else if !allowed && tokens < 1 {
			// t.Logf("[INFO]: Successfully did not allow a request, not enough tokens")
		} else {
			t.Log(i, allowed, tokens)
			t.Errorf("[ERROR]: Allowed a request but there were not enough tokens")
		}
	}
}

func Test_Allow_With_Sleep(t *testing.T) {
	tb := NewTokenBucket(5, 1)

	for i := range 7 {
		tokens := tb.Tokens
		allowed := tb.Allow()

		if allowed && tokens >= 1 {
			// t.Logf("[INFO]: Successfully allowed a request")
		} else if !allowed && tokens < 1 {
			// t.Logf("[INFO]: Successfully did not allow a request, not enough tokens")
		} else {
			t.Log(i, allowed, tokens)
			t.Errorf("[ERROR]: Allowed a request but there were not enough tokens")
		}

		time.Sleep(1 * time.Second)
	}
}

func Test_Conccurent_Calls(t *testing.T) {
	tb := NewTokenBucket(50, 0)

	wg := sync.WaitGroup{}
	var successful atomic.Int64
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worked := tb.Allow()
			if worked {
				successful.Add(1)
			}
		}()
	}

	wg.Wait()

	successes := successful.Load()

	if successes == 50 {
		// t.Log("[INFO]: All worked")
	} else {
		t.Errorf("[ERROR]: Only %v were successful", successes)
	}
}
