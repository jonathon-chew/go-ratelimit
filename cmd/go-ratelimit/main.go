package goratelimit

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// TokenBucket limits requests using a token-bucket algorithm.
//
// A bucket starts full and refills over time up to its configured capacity.
type TokenBucket struct {
	mu         sync.RWMutex
	capacity   float64
	tokens     float64
	refillRate float64
	lastRefill time.Time
}

// refill adds tokens based on the time elapsed since the previous refill.
func (tb *TokenBucket) refill() {
	// Calculate how much time has passed since lastRefill.
	var seconds_passed = time.Since(tb.lastRefill).Seconds()
	// Convert that elapsed time into tokens using refillRate.
	amount_of_tokens_to_refresh := seconds_passed * tb.refillRate
	// Add those tokens to tokens.
	tb.tokens += amount_of_tokens_to_refresh
	// Ensure tokens never exceeds capacity.
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	// Update lastRefill.
	tb.lastRefill = time.Now()
}

// NewTokenBucket creates a full token bucket with the given capacity and
// refill rate in tokens per second. Values below one are normalized to one;
// NaN and infinite values return an error.
func NewTokenBucket(capacity, refillRate float64) (*TokenBucket, error) {

	var tokenBucket TokenBucket

	if math.IsNaN(capacity) {
		return nil, fmt.Errorf("capacity is NaN")
	}

	if math.IsNaN(refillRate) {
		return nil, fmt.Errorf("refillRate is NaN")
	}

	if math.IsInf(capacity, 0) {
		return nil, fmt.Errorf("capacity is infinte")
	}

	if math.IsInf(refillRate, 0) {
		return nil, fmt.Errorf("refillRate is infinte")
	}

	capacity = max(capacity, 1.0)
	refillRate = max(refillRate, 1.0)

	tokenBucket.capacity = capacity
	tokenBucket.tokens = capacity
	tokenBucket.refillRate = refillRate
	tokenBucket.lastRefill = time.Now()

	return &tokenBucket, nil
}

// Allow consumes one token if one is available and reports whether it succeeded.
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

// AllowN consumes n tokens if enough tokens are available and reports whether
// it succeeded. Zero succeeds without consuming tokens; negative and NaN
// values are rejected.
func (tb *TokenBucket) AllowN(n float64) bool {

	if n < 0 {
		return false
	}

	if math.IsNaN(n) {
		return false
	}

	// Acquire the mutex.
	tb.mu.Lock()
	defer tb.mu.Unlock()

	// Refill the bucket using your internal refill() method.
	tb.refill()

	// Check whether at least one token is available.
	// If no tokens are available, return false.
	if tb.tokens < n {
		return false
	}
	// Otherwise, consume n tokens.
	tb.tokens -= n

	// Return true.
	return true
}

// Limit returns the bucket's maximum token capacity.
func (tb *TokenBucket) Limit() float64 {
	tb.mu.RLock()
	defer tb.mu.RUnlock()

	return tb.capacity
}

// Refill updates the bucket with tokens accrued since the previous refill.
func (tb *TokenBucket) Refill() error {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	return nil
}

// Remaining returns the number of tokens consumed from the bucket's capacity.
func (tb *TokenBucket) Remaining() float64 {
	tb.mu.RLock()
	defer tb.mu.RUnlock()

	return tb.capacity - tb.tokens
}

// TokensAvaliable returns the number of tokens currently available.
func (tb *TokenBucket) TokensAvaliable() float64 {
	tb.mu.RLock()
	defer tb.mu.RUnlock()

	return tb.tokens
}
