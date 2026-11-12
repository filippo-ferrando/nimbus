// Package retry implements exponential backoff with full jitter.
package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Policy configures retries. Attempts counts the first try.
type Policy struct {
	Attempts int
	Base     time.Duration
	Max      time.Duration
}

// Permanent wraps an error that must not be retried.
type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent marks err as non-retryable.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanent{err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p permanent
	return errors.As(err, &p)
}

// Delay returns the full-jitter backoff for the given (0-based) failed attempt:
// a random duration in [0, min(Max, Base*2^attempt)], never below Base/4.
func (p Policy) Delay(attempt int) time.Duration {
	ceil := p.Base
	for i := 0; i < attempt && ceil < p.Max; i++ {
		ceil *= 2
	}
	if ceil > p.Max {
		ceil = p.Max
	}
	d := time.Duration(rand.Int64N(int64(ceil) + 1))
	if min := p.Base / 4; d < min {
		d = min
	}
	return d
}

// Do runs fn until it succeeds, returns a permanent error, the context ends or
// attempts are exhausted. onRetry is called before each sleep.
func Do(ctx context.Context, p Policy, fn func(attempt int) error, onRetry func(attempt int, err error, wait time.Duration)) error {
	if p.Attempts < 1 {
		p.Attempts = 1
	}
	var err error
	for a := 0; a < p.Attempts; a++ {
		if err = fn(a); err == nil {
			return nil
		}
		if IsPermanent(err) || ctx.Err() != nil || a == p.Attempts-1 {
			break
		}
		wait := p.Delay(a)
		if onRetry != nil {
			onRetry(a+1, err, wait)
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
