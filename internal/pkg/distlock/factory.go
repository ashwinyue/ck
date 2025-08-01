package distlock

import (
	"fmt"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
)

// LockerType defines the type of distributed locker.
type LockerType string

const (
	// LockerTypeRedis uses basic Redis implementation.
	LockerTypeRedis LockerType = "redis"
	// LockerTypeRedsync uses redsync implementation.
	LockerTypeRedsync LockerType = "redsync"
)

// Factory creates distributed lockers.
type Factory struct {
	client *redis.Client
	logger *log.Helper
}

// NewFactory creates a new locker factory.
func NewFactory(client *redis.Client, logger *log.Helper) *Factory {
	if logger == nil {
		logger = log.NewHelper(log.DefaultLogger)
	}
	return &Factory{
		client: client,
		logger: logger,
	}
}

// CreateLocker creates a new distributed locker of the specified type.
func (f *Factory) CreateLocker(lockerType LockerType, opts ...Option) (Locker, error) {
	switch lockerType {
	case LockerTypeRedis:
		return NewRedisLocker(f.client, opts...), nil
	case LockerTypeRedsync:
		return NewRedsyncLocker(f.client, opts...), nil
	default:
		return nil, fmt.Errorf("unsupported locker type: %s", lockerType)
	}
}

// CreateRedisLocker creates a Redis-based distributed locker.
func (f *Factory) CreateRedisLocker(opts ...Option) Locker {
	return NewRedisLocker(f.client, opts...)
}

// CreateRedsyncLocker creates a redsync-based distributed locker.
func (f *Factory) CreateRedsyncLocker(opts ...Option) Locker {
	return NewRedsyncLocker(f.client, opts...)
}
