package distlock

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

// Locker defines the interface for distributed locking mechanisms.
type Locker interface {
	// Lock attempts to acquire the distributed lock.
	Lock(ctx context.Context) error
	// Unlock releases the distributed lock.
	Unlock(ctx context.Context) error
	// Renew refreshes the lock's expiration time.
	Renew(ctx context.Context) error
}

// Option defines configuration options for lockers.
type Option func(*Options)

// Options holds configuration for distributed lockers.
type Options struct {
	lockName    string
	lockTimeout time.Duration
	ownerID     string
	logger      *log.Helper
}

// WithLockName sets the lock name.
func WithLockName(name string) Option {
	return func(o *Options) {
		o.lockName = name
	}
}

// WithLockTimeout sets the lock timeout duration.
func WithLockTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.lockTimeout = timeout
	}
}

// WithOwnerID sets the owner ID for the lock.
func WithOwnerID(ownerID string) Option {
	return func(o *Options) {
		o.ownerID = ownerID
	}
}

// WithLogger sets the logger for the locker.
func WithLogger(logger *log.Helper) Option {
	return func(o *Options) {
		o.logger = logger
	}
}

// ApplyOptions applies the given options and returns the final Options.
func ApplyOptions(opts ...Option) *Options {
	o := &Options{
		lockName:    "default_lock",
		lockTimeout: 30 * time.Second,
		ownerID:     "default_owner",
		logger:      log.NewHelper(log.DefaultLogger),
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}
