package distlock

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/google/uuid"
)

// RedsyncLocker provides a distributed locking mechanism using redsync.
type RedsyncLocker struct {
	redsync  *redsync.Redsync
	mutex    *redsync.Mutex
	lockName string
	ownerID  string
	logger   *log.Helper
	options  []redsync.Option
}

// Ensure RedsyncLocker implements the Locker interface.
var _ Locker = (*RedsyncLocker)(nil)

// NewRedsyncLocker creates a new RedsyncLocker instance.
func NewRedsyncLocker(client *redis.Client, opts ...Option) *RedsyncLocker {
	o := ApplyOptions(opts...)

	// Generate a unique owner ID if not provided
	if o.ownerID == "default_owner" {
		o.ownerID = uuid.New().String()
	}

	// Create redsync pool
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	// Configure redsync options
	redsyncOpts := []redsync.Option{
		redsync.WithExpiry(o.lockTimeout),
		redsync.WithTries(3),
		redsync.WithRetryDelay(100 * time.Millisecond),
		redsync.WithDriftFactor(0.01),
		redsync.WithTimeoutFactor(0.05),
		redsync.WithGenValueFunc(func() (string, error) {
			return o.ownerID, nil
		}),
	}

	locker := &RedsyncLocker{
		redsync:  rs,
		lockName: o.lockName,
		ownerID:  o.ownerID,
		logger:   o.logger,
		options:  redsyncOpts,
	}

	locker.logger.Infow("RedsyncLocker initialized", "lockName", locker.lockName, "ownerID", locker.ownerID)
	return locker
}

// Lock attempts to acquire the distributed lock.
func (l *RedsyncLocker) Lock(ctx context.Context) error {
	// Create a new mutex for this lock attempt
	l.mutex = l.redsync.NewMutex(l.lockName, l.options...)

	err := l.mutex.LockContext(ctx)
	if err != nil {
		l.logger.Errorw("Failed to acquire lock", "error", err, "lockName", l.lockName)
		return err
	}

	l.logger.Infow("Lock acquired", "ownerID", l.ownerID, "lockName", l.lockName)
	return nil
}

// Unlock releases the distributed lock.
func (l *RedsyncLocker) Unlock(ctx context.Context) error {
	if l.mutex == nil {
		l.logger.Warnw("No mutex to unlock", "lockName", l.lockName)
		return nil
	}

	ok, err := l.mutex.UnlockContext(ctx)
	if err != nil {
		l.logger.Errorw("Failed to release lock", "error", err, "lockName", l.lockName)
		return err
	}

	if !ok {
		l.logger.Warnw("Lock was not owned by this instance", "ownerID", l.ownerID, "lockName", l.lockName)
	}

	l.logger.Infow("Lock released", "ownerID", l.ownerID, "lockName", l.lockName, "success", ok)
	l.mutex = nil
	return nil
}

// Renew refreshes the lock's expiration time.
func (l *RedsyncLocker) Renew(ctx context.Context) error {
	if l.mutex == nil {
		l.logger.Warnw("No mutex to renew", "lockName", l.lockName)
		return nil
	}

	ok, err := l.mutex.ExtendContext(ctx)
	if err != nil {
		l.logger.Errorw("Failed to renew lock", "error", err, "lockName", l.lockName)
		return err
	}

	if !ok {
		l.logger.Warnw("Lock renewal failed - not owned by this instance", "ownerID", l.ownerID, "lockName", l.lockName)
	}

	l.logger.Infow("Lock renewed", "ownerID", l.ownerID, "lockName", l.lockName, "success", ok)
	return nil
}

// GetOwnerID returns the owner ID of this locker instance.
func (l *RedsyncLocker) GetOwnerID() string {
	return l.ownerID
}

// GetLockName returns the lock name.
func (l *RedsyncLocker) GetLockName() string {
	return l.lockName
}

// IsLocked returns whether the lock is currently held.
func (l *RedsyncLocker) IsLocked() bool {
	return l.mutex != nil
}
