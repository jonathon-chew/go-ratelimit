package main

import (
	"sync"
	"time"
)

type TokenBucket struct {
	mu         sync.Mutex
	Capacity   float64
	Tokens     float64
	RefillRate float64
	LastRefill time.Time
}

func (tb *TokenBucket) refill() {
	// Calculate how much time has passed since LastRefill.
	var seconds_passed = time.Since(tb.LastRefill).Seconds()
	// Convert that elapsed time into tokens using RefillRate.
	amount_of_tokens_to_refresh := seconds_passed * tb.RefillRate
	// Add those tokens to Tokens.
	tb.Tokens += amount_of_tokens_to_refresh
	// Ensure Tokens never exceeds Capacity.
	if tb.Tokens > tb.Capacity {
		tb.Tokens = tb.Capacity
	}
	// Update LastRefill.
	tb.LastRefill = time.Now()
}

func NewTokenBucket(Capacity, RefillRate float64) *TokenBucket {

	var tokenBucket TokenBucket

	tokenBucket.Capacity = Capacity
	tokenBucket.Tokens = Capacity
	tokenBucket.RefillRate = RefillRate
	tokenBucket.LastRefill = time.Now()

	return &tokenBucket
}

func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

func (tb *TokenBucket) AllowN(n float64) bool {

	if n < 0 {
		return false
	}

	// Acquire the mutex.
	tb.mu.Lock()
	defer tb.mu.Unlock()
	// Refill the bucket using your internal refill() method.
	tb.refill()
	// Check whether at least one token is available.
	// If no tokens are available, return false.
	if tb.Tokens < n {
		return false
	}
	// Otherwise, consume one token.
	tb.Tokens -= n
	// Return true.
	return true
}

func (tb *TokenBucket) Refill() error {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	return nil
}

func (tb *TokenBucket) Remaining() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	return tb.Capacity - tb.Tokens
}
