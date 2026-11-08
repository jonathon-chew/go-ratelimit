# go-ratelimit

A small Go implementation of a thread-safe token-bucket rate limiter.

## What it does

`TokenBucket` starts full, allows requests while tokens are available, and refills tokens over time at a configured rate up to its capacity. Calls are protected by a mutex so the same bucket can be shared between concurrent goroutines.

The package currently exposes:

- `NewTokenBucket(capacity, refillRate)` to create a bucket
- `Allow()` to consume one token when available
- `AllowN(n)` to consume a requested number of tokens
- `Refill()` to update the bucket from elapsed time
- `Remaining()` to inspect how many tokens have been consumed from capacity

## Example

```go
bucket := NewTokenBucket(10, 2) // capacity 10, refill 2 tokens/second

if bucket.Allow() {
	// process the request
}
```

## Testing

Run the tests from the repository root:

```bash
go test ./...
```

The tests cover initial capacity, refill behaviour, and concurrent calls.

## Current scope

This is a focused learning implementation rather than a complete HTTP middleware package. It does not yet provide HTTP handlers, distributed coordination, or configurable burst policies beyond the token-bucket capacity and refill rate.

## License

See [LICENSE](./LICENSE) for details.
