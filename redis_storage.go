package redis_storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/golang-infrastructure/go-iterator"
	"github.com/redis/go-redis/v9"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

// ------------------------------------------------ ---------------------------------------------------------------------

// Lua 脚本：原子 CAS 操作
// Redis 的 SET NX 不支持多字段+条件判断，Lua 保证"读版本+写"原子

const (
	// createScript 原子创建：key 不存在时写入 version 和 json，返回 1；已存在返回 0
	// 写入成功后向 KEYS[2]（watch 频道）PUBLISH 新的 json，驱动 Watch() 的通知
	createScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('HSET', KEYS[1], 'version', ARGV[1], 'json', ARGV[2])
    redis.call('PUBLISH', KEYS[2], ARGV[2])
    return 1
else
    return 0
end
`

	// updateScript 原子更新：当前 version 匹配时写入新 version 和 json，返回 1；不匹配返回 0
	// 写入成功后向 KEYS[2]（watch 频道）PUBLISH 新的 json，驱动 Watch() 的通知
	updateScript = `
if redis.call('HGET', KEYS[1], 'version') == ARGV[1] then
    redis.call('HSET', KEYS[1], 'version', ARGV[2], 'json', ARGV[3])
    redis.call('PUBLISH', KEYS[2], ARGV[3])
    return 1
else
    return 0
end
`

	// deleteScript 原子删除：当前 version 匹配时删除 key，返回 1；不匹配返回 0
	// 删除成功后向 KEYS[2]（watch 频道）PUBLISH 空字符串，通知锁已释放
	deleteScript = `
if redis.call('HGET', KEYS[1], 'version') == ARGV[1] then
    redis.call('DEL', KEYS[1])
    redis.call('PUBLISH', KEYS[2], '')
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
// Redis 通过 Lua 脚本实现原子 CAS，通过 TIME 命令提供服务端时间，Lua DEL 实现真删，
// 且支持 Keyspace Notifications 作为 Watch 能力。
func (x *RedisStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
		storage.CapabilityAtomicDelete,
		storage.CapabilityWatch,
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
	result, err := x.client.Eval(ctx, createScript, []string{key, x.publishChannel(lockId)},
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
	result, err := x.client.Eval(ctx, updateScript, []string{key, x.publishChannel(lockId)},
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
	result, err := x.client.Eval(ctx, deleteScript, []string{key, x.publishChannel(lockId)},
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

// Watch 监听指定锁的状态变更。
// 首选基于 Redis Pub/Sub 的轻量通知通道；失败时回退为 Keyevent 通知。
func (x *RedisStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	channel, err := x.tryWatchWithPublish(ctx, lockId)
	if err == nil {
		return channel, nil
	}

	channel, pubSubErr := x.tryWatchWithKeyevent(ctx, lockId)
	if pubSubErr == nil {
		return channel, nil
	}

	return nil, fmt.Errorf("redis watch: publish=%w, keyevent=%w", err, pubSubErr)
}

func (x *RedisStorage) tryWatchWithPublish(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	pubsub := x.client.Subscribe(ctx, x.publishChannel(lockId))
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, err
	}

	channel := make(chan *storage.LockInformation, 1)
	go func() {
		defer close(channel)
		defer pubsub.Close()

		ch := pubsub.Channel()
		sent := false
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				if msg == nil {
					continue
				}
				info, parseErr := x.parseWatchPayload(msg.Payload)
				if parseErr != nil {
					continue
				}
				if !sent || info != nil {
					sent = true
					select {
					case <-ctx.Done():
						return
					case channel <- info:
					}
				}
			}
		}
	}()

	return channel, nil
}

func (x *RedisStorage) tryWatchWithKeyevent(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	pubsub := x.client.Subscribe(ctx, x.keyeventChannel(lockId))
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, err
	}

	channel := make(chan *storage.LockInformation, 1)
	go func() {
		defer close(channel)
		defer pubsub.Close()

		ch := pubsub.Channel()
		lastInfo, _ := x.currentLockInformation(ctx, lockId)
		if lastInfo != nil {
			select {
			case <-ctx.Done():
				return
			case channel <- lastInfo:
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				if msg == nil {
					continue
				}
				info, _ := x.currentLockInformation(ctx, lockId)
				select {
				case <-ctx.Done():
					return
				case channel <- info:
				}
			}
		}
	}()

	return channel, nil
}

func (x *RedisStorage) publishChannel(lockId string) string {
	return x.options.KeyPrefix + ":watch:" + lockId
}

// keyeventChannel 兜底路径监听的 keyspace-notification 事件频道。
// CAS 通过 Lua HSET 写入锁记录，对应的 Redis keyspace-notification 事件类型是
// hset，不是 set（此前订阅的是 set，与实际写入命令的事件名不匹配，永远收不到通知）。
func (x *RedisStorage) keyeventChannel(lockId string) string {
	db := 0
	if x.client != nil && x.client.Options() != nil {
		db = x.client.Options().DB
	}
	return fmt.Sprintf("__keyevent@%d__:hset", db)
}

func (x *RedisStorage) parseWatchPayload(payload string) (*storage.LockInformation, error) {
	if strings.TrimSpace(payload) == "" {
		return nil, nil
	}
	info := &storage.LockInformation{}
	if err := json.Unmarshal([]byte(payload), info); err != nil {
		return nil, err
	}
	return info, nil
}

func (x *RedisStorage) currentLockInformation(ctx context.Context, lockId string) (*storage.LockInformation, error) {
	raw, err := x.Get(ctx, lockId)
	if err != nil {
		// Watch 快照场景：锁不存在表示"当前没有锁"，返回 nil 而非错误。
		if errors.Is(err, storage_lock.ErrLockNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	info := &storage.LockInformation{}
	if err := json.Unmarshal([]byte(raw), info); err != nil {
		return nil, err
	}
	return info, nil
}

// ------------------------------------------------ ---------------------------------------------------------------------
