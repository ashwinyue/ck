package distlock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

// RedisLocker provides a distributed locking mechanism using Redis.
type RedisLocker struct {
	client      *redis.Client
	lockName    string
	lockTimeout time.Duration
	renewTicker *time.Ticker
	stopChan    chan struct{}
	mu          sync.Mutex
	ownerID     string
	logger      *log.Helper
}

// Ensure RedisLocker implements the Locker interface.
var _ Locker = (*RedisLocker)(nil)

// NewRedisLocker creates a new RedisLocker instance.
func NewRedisLocker(client *redis.Client, opts ...Option) *RedisLocker {
	o := ApplyOptions(opts...)

	// Generate a unique owner ID if not provided
	if o.ownerID == "default_owner" {
		o.ownerID = uuid.New().String()
	}

	locker := &RedisLocker{
		client:      client,
		lockName:    o.lockName,
		lockTimeout: o.lockTimeout,
		stopChan:    make(chan struct{}),
		ownerID:     o.ownerID,
		logger:      o.logger,
	}

	locker.logger.Infow("RedisLocker initialized", "lockName", locker.lockName, "ownerID", locker.ownerID)
	return locker
}

// Lock attempts to acquire the distributed lock.
func (l *RedisLocker) Lock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	success, err := l.client.SetNX(ctx, l.lockName, l.ownerID, l.lockTimeout).Result()
	if err != nil {
		l.logger.Errorw("Failed to set lock", "error", err)
		return err
	}
	if !success {
		currentOwnerID, err := l.client.Get(ctx, l.lockName).Result()
		if err != nil {
			l.logger.Errorw("Failed to get current owner ID", "error", err)
			return err
		}
		if currentOwnerID != l.ownerID {
			l.logger.Warnw("Lock is already held by another owner", "currentOwnerID", currentOwnerID)
			return fmt.Errorf("lock is already held by %s", currentOwnerID)
		}
		l.logger.Infow("Lock is already held by the current owner, extending the lock if needed")
		return nil
	}

	l.renewTicker = time.NewTicker(l.lockTimeout / 2)
	go l.renewLock(ctx)

	l.logger.Infow("Lock acquired", "ownerID", l.ownerID)
	return nil
}

// Unlock releases the distributed lock.
func (l *RedisLocker) Unlock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.renewTicker != nil {
		l.renewTicker.Stop()
		l.renewTicker = nil
		close(l.stopChan)
		l.stopChan = make(chan struct{}) // Reset for potential reuse
		l.logger.Infow("Stopped renewing lock", "lockName", l.lockName)
	}

	// Use Lua script to ensure atomic unlock operation
	lua := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`
	result, err := l.client.Eval(ctx, lua, []string{l.lockName}, l.ownerID).Result()
	if err != nil {
		l.logger.Errorw("Failed to delete lock", "error", err)
		return err
	}

	if result.(int64) == 0 {
		l.logger.Warnw("Lock was not owned by this instance", "ownerID", l.ownerID)
		return fmt.Errorf("lock was not owned by this instance")
	}

	l.logger.Infow("Lock released", "ownerID", l.ownerID)
	return nil
}

// Renew refreshes the lock's expiration time.
func (l *RedisLocker) Renew(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Use Lua script to ensure atomic renew operation
	lua := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("expire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`
	result, err := l.client.Eval(ctx, lua, []string{l.lockName}, l.ownerID, int(l.lockTimeout.Seconds())).Result()
	if err != nil {
		l.logger.Errorw("Failed to renew lock", "error", err)
		return err
	}

	if result.(int64) == 0 {
		l.logger.Warnw("Lock was not owned by this instance during renewal", "ownerID", l.ownerID)
		return fmt.Errorf("lock was not owned by this instance")
	}

	l.logger.Infow("Lock renewed", "ownerID", l.ownerID)
	return nil
}

// renewLock periodically renews the lock.
func (l *RedisLocker) renewLock(ctx context.Context) {
	for {
		select {
		case <-l.stopChan:
			return
		case <-ctx.Done():
			return
		case <-l.renewTicker.C:
			if err := l.Renew(ctx); err != nil {
				l.logger.Errorw("Failed to renew lock", "error", err)
				// If renewal fails, stop the ticker to prevent further attempts
				l.renewTicker.Stop()
				return
			}
		}
	}
}

// GetOwnerID returns the owner ID of this locker instance.
func (l *RedisLocker) GetOwnerID() string {
	return l.ownerID
}

// GetLockName returns the lock name.
func (l *RedisLocker) GetLockName() string {
	return l.lockName
}
