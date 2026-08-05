package redis_storage

import (
	"github.com/storage-lock/go-storage"
	"github.com/redis/go-redis/v9"
)

// RedisStorageOptions Redis 的存储选项
type RedisStorageOptions struct {

	// 获取连接
	ConnectionManager storage.ConnectionManager[*redis.Client]

	// KeyPrefix Redis 中 key 的前缀，用于隔离不同的锁实例
	// 实际的 key 格式为：<KeyPrefix>:lock:<lockId>
	KeyPrefix string
}

func NewRedisStorageOptions() *RedisStorageOptions {
	return &RedisStorageOptions{
		KeyPrefix: storage.DefaultStorageTableName,
	}
}

func NewRedisStorageOptionsWithURI(uri string) *RedisStorageOptions {
	return NewRedisStorageOptions().SetConnectionManager(NewRedisConnectionManagerFromURI(uri))
}

func (x *RedisStorageOptions) SetKeyPrefix(keyPrefix string) *RedisStorageOptions {
	x.KeyPrefix = keyPrefix
	return x
}

func (x *RedisStorageOptions) SetConnectionManager(connectionManager storage.ConnectionManager[*redis.Client]) *RedisStorageOptions {
	x.ConnectionManager = connectionManager
	return x
}

func (x *RedisStorageOptions) Check() error {
	if x.KeyPrefix == "" {
		return ErrKeyPrefixEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}
