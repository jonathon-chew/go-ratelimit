package goratelimit

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenBucketExactCapacityExhaustion(t *testing.T) {
	tb, err := NewTokenBucket(3, 0)
	if err != nil {
		t.Fatalf("NewTokenBucket returned an error: %v", err)
	}

	for i := range 3 {
		if !tb.Allow() {
			t.Fatalf("Allow() failed for token %d of %d", i+1, 3)
		}
	}
	if tb.Allow() {
		t.Fatal("Allow() succeeded after the bucket was exhausted")
	}
	if got := tb.TokensAvaliable(); got > 0.001 {
		t.Fatalf("TokensAvaliable() = %v, want approximately 0", got)
	}
}

func TestTokenBucketFractionalTokens(t *testing.T) {
	tb, err := NewTokenBucket(1.5, 0)
	if err != nil {
		t.Fatalf("NewTokenBucket returned an error: %v", err)
	}

	if !tb.AllowN(0.5) || !tb.AllowN(0.5) || !tb.AllowN(0.5) {
		t.Fatal("AllowN() rejected a request that fit in the fractional capacity")
	}
	// Freeze refilling so the exhaustion assertion is independent of scheduler timing.
	tb.refillRate = 0
	if tb.AllowN(0.5) {
		t.Fatal("AllowN() accepted a request larger than the remaining fractional tokens")
	}
}

func TestTokenBucketRefillAndCapping(t *testing.T) {
	tb, err := NewTokenBucket(5, 2)
	if err != nil {
		t.Fatalf("NewTokenBucket returned an error: %v", err)
	}
	if !tb.AllowN(4) {
		t.Fatal("AllowN(4) failed")
	}

	tb.lastRefill = time.Now().Add(-time.Second)
	if err := tb.Refill(); err != nil {
		t.Fatalf("Refill returned an error: %v", err)
	}
	if got := tb.TokensAvaliable(); got < 3 || got > 3.01 {
		t.Fatalf("TokensAvaliable() after refill = %v, want approximately 3", got)
	}

	tb.lastRefill = time.Now().Add(-10 * time.Second)
	if err := tb.Refill(); err != nil {
		t.Fatalf("Refill returned an error: %v", err)
	}
	if got := tb.TokensAvaliable(); got != tb.Limit() {
		t.Fatalf("TokensAvaliable() = %v after over-refill, want capacity %v", got, tb.Limit())
	}
}

func TestTokenBucketZeroAndInvalidRates(t *testing.T) {
	for _, rate := range []float64{0, -1, math.Inf(-1)} {
		tb, err := NewTokenBucket(2, rate)
		if math.IsInf(rate, 0) {
			if err == nil {
				t.Errorf("NewTokenBucket(%v) unexpectedly succeeded", rate)
			}
			continue
		}
		if err != nil {
			t.Fatalf("NewTokenBucket(%v) returned an error: %v", rate, err)
		}
		if tb.refillRate != 1 {
			t.Errorf("refillRate for %v = %v, want 1", rate, tb.refillRate)
		}
	}

	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := NewTokenBucket(2, value); err == nil {
			t.Errorf("NewTokenBucket(%v) unexpectedly succeeded", value)
		}
	}
}

func TestTokenBucketAllowNInvalidValues(t *testing.T) {
	tb, err := NewTokenBucket(2, 0)
	if err != nil {
		t.Fatalf("NewTokenBucket returned an error: %v", err)
	}

	for _, n := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if n == 0 {
			if !tb.AllowN(n) {
				t.Error("AllowN(0) = false, want true")
			}
			continue
		}
		if tb.AllowN(n) {
			t.Errorf("AllowN(%v) = true, want false", n)
		}
	}
	if got := tb.TokensAvaliable(); got != 2 {
		t.Fatalf("invalid AllowN calls changed token count to %v, want 2", got)
	}
}

func TestTokenBucketConcurrentCallers(t *testing.T) {
	const capacity = 100
	tb, err := NewTokenBucket(capacity, 0)
	if err != nil {
		t.Fatalf("NewTokenBucket returned an error: %v", err)
	}

	var wg sync.WaitGroup
	var allowed atomic.Int64
	for range capacity * 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tb.Allow() {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := allowed.Load(); got != capacity {
		t.Fatalf("successful concurrent calls = %d, want %d", got, capacity)
	}
}

func TestTokenBucketLongIdlePeriod(t *testing.T) {
	tb, err := NewTokenBucket(4, 1)
	if err != nil {
		t.Fatalf("NewTokenBucket returned an error: %v", err)
	}
	if !tb.AllowN(3) {
		t.Fatal("AllowN(3) failed")
	}

	tb.lastRefill = time.Now().Add(-365 * 24 * time.Hour)
	if err := tb.Refill(); err != nil {
		t.Fatalf("Refill returned an error: %v", err)
	}
	if got := tb.TokensAvaliable(); got != tb.Limit() {
		t.Fatalf("TokensAvaliable() after long idle period = %v, want capacity %v", got, tb.Limit())
	}
}

func Test_Allow(t *testing.T) {
	tb, err := NewTokenBucket(5, 1)
	if err != nil {
		t.Error("[ERROR]: Making new bucket")
	}

	for i := range 7 {
		tokens := tb.tokens
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
	tb, err := NewTokenBucket(5, 1)
	if err != nil {
		t.Error("[ERROR]: Making new bucket")
	}
	for i := range 7 {
		tokens := tb.tokens
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
	tb, err := NewTokenBucket(50, 0)
	if err != nil {
		t.Error("[ERROR]: Making new bucket")
	}

	wg := sync.WaitGroup{}
	var successful atomic.Int64
	for range 100 {
		wg.Go(func() {
			worked := tb.Allow()
			if worked {
				successful.Add(1)
			}
		})
	}

	wg.Wait()

	successes := successful.Load()

	if successes == 50 {
		// t.Log("[INFO]: All worked")
	} else {
		t.Errorf("[ERROR]: Only %v were successful", successes)
	}
}
