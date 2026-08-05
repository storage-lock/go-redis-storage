package redis_storage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

// ------------------------------------------------ ---------------------------------------------------------------------

// Lua 脚本：原子 CAS 操作
// Redis 的 SET NX 不支持多字段+条件判断，Lua 保证"读版本+写"原子

const (
	// createScript 原子创建：key 不存在时写入 version 和 json，返回 1；已存在返回 0
	createScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('HSET', KEYS[1], 'version', ARGV[1], 'json', ARGV[2])
    return 1
else
    return 0
end
`

	// updateScript 原子更新：当前 version 匹配时写入新 version 和 json，返回 1；不匹配返回 0
	updateScript = `
if redis.call('HGET', KEYS[1], 'version') == ARGV[1] then
    redis.call('HSET', KEYS[1], 'version', ARGV[2], 'json', ARGV[3])
    return 1
else
    return 0
end
`

	// deleteScript 原子删除：当前 version 匹配时删除 key，返回 1；不匹配返回 0
	deleteScript = `
if redis.call('HGET', KEYS[1], 'version') == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
else
    return 0
end
`
)

// ------------------------------------------------ ---------------------------------------------------------------------

// RedisStorage 基于 Redis 的存储引擎实现
type RedisStorage struct {
	options *RedisStorageOptions
	client  *redis.Client
}

var _ storage.Storage = &RedisStorage{}

// NewRedisStorage 创建一个基于 Redis 的存储引擎
func NewRedisStorage(ctx context.Context, options *RedisStorageOptions) (*RedisStorage, error) {

	if err := options.Check(); err != nil {
		return nil, err
	}

	s := &RedisStorage{
		options: options,
	}

	err := s.Init(ctx)
	if err != nil {
		return nil, err
	}

	return s, nil
}

const RedisStorageName = "redis-storage"

func (x *RedisStorage) GetName() string {
	return RedisStorageName
}

// Capabilities 声明 Redis 存储支持的能力
// Redis 通过 Lua 脚本实现原子 CAS，通过 TIME 命令提供服务端时间，Lua DEL 实现真删
func (x *RedisStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
		storage.CapabilityAtomicDelete,
	}
}

func (x *RedisStorage) Init(ctx context.Context) error {
	client, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	// 探活幂等：确保连接可用
	if err := client.Ping(ctx).Err(); err != nil {
		return err
	}
	x.client = client
	return nil
}

// lockKey 返回 Redis 中存储锁信息的 Hash key
func (x *RedisStorage) lockKey(lockId string) string {
	return fmt.Sprintf("%s:lock:%s", x.options.KeyPrefix, lockId)
}

// CreateWithVersion 原子创建锁记录
// Lua 脚本保证"检查 key 不存在 + 写入"是原子操作，不存在 Check-Then-Act 竞态
func (x *RedisStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	key := x.lockKey(lockId)
	result, err := x.client.Eval(ctx, createScript, []string{key},
		strconv.FormatUint(uint64(version), 10),
		lockInformation.ToJsonString(),
	).Int64()
	if err != nil {
		return err
	}
	if result == 0 {
		return storage_lock.ErrVersionMiss
	}
	return nil
}

// UpdateWithVersion 原子更新锁记录
// Lua 脚本保证"读版本 + 写"是原子操作，不存在 Check-Then-Act 竞态
func (x *RedisStorage) UpdateWithVersion(ctx context.Context, lockId string, exceptedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.lockKey(lockId)
	result, err := x.client.Eval(ctx, updateScript, []string{key},
		strconv.FormatUint(uint64(exceptedVersion), 10),
		strconv.FormatUint(uint64(newVersion), 10),
		lockInformation.ToJsonString(),
	).Int64()
	if err != nil {
		return err
	}
	if result == 0 {
		return storage_lock.ErrVersionMiss
	}
	return nil
}

// DeleteWithVersion 原子删除锁记录
// Lua 脚本保证"读版本 + 删除"是原子操作，不存在 Check-Then-Act 竞态
func (x *RedisStorage) DeleteWithVersion(ctx context.Context, lockId string, exceptedVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.lockKey(lockId)
	result, err := x.client.Eval(ctx, deleteScript, []string{key},
		strconv.FormatUint(uint64(exceptedVersion), 10),
	).Int64()
	if err != nil {
		return err
	}
	if result == 0 {
		return storage_lock.ErrVersionMiss
	}
	return nil
}

// Get 获取锁信息
func (x *RedisStorage) Get(ctx context.Context, lockId string) (string, error) {
	key := x.lockKey(lockId)
	result, err := x.client.HGet(ctx, key, "json").Result()
	if err != nil {
		if err == redis.Nil {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}
	return result, nil
}

// GetTime 获取 Redis 服务端时间
// 使用 Redis TIME 命令获取服务端时间，满足 CapabilityReliableTime
func (x *RedisStorage) GetTime(ctx context.Context) (time.Time, error) {
	return x.client.Time(ctx).Result()
}

// Close 关闭存储引擎
// 连接由 ConnectionManager 管理，Storage 只清空引用
func (x *RedisStorage) Close(ctx context.Context) error {
	x.client = nil
	return nil
}

// List 列出当前所有的锁信息
// 使用 SCAN 匹配 <KeyPrefix>:lock:* 模式 + HGETALL 收集
// 注意：SCAN 非原子快照，仅运维查看，不参与互斥性
func (x *RedisStorage) List(ctx context.Context) (iterator.Iterator[*storage.LockInformation], error) {
	pattern := x.options.KeyPrefix + ":lock:*"
	var keys []string
	var cursor uint64
	for {
		var batch []string
		var err error
		batch, cursor, err = x.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		if cursor == 0 {
			break
		}
	}

	slice := make([]*storage.LockInformation, 0, len(keys))
	for _, key := range keys {
		vals, err := x.client.HGetAll(ctx, key).Result()
		if err != nil {
			// key 可能已被并发删除，跳过
			continue
		}
		jsonStr, ok := vals["json"]
		if !ok {
			continue
		}
		info := &storage.LockInformation{}
		if err := json.Unmarshal([]byte(jsonStr), info); err != nil {
			// 数据损坏，跳过
			continue
		}
		slice = append(slice, info)
	}
	return iterator.FromSlice(slice), nil
}

// ------------------------------------------------ ---------------------------------------------------------------------
