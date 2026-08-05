package redis_storage

import (
	"context"
	"github.com/storage-lock/go-storage"
	"github.com/redis/go-redis/v9"
	"sync"
)

// RedisConnectionManager 负责维护与 Redis 的连接
type RedisConnectionManager struct {

	// 连接到 Redis 的地址
	URI string

	// Redis 客户端选项（与 URI 互斥，优先使用 Options）
	Options *redis.Options

	// initMu 保护下面的初始化状态。原用 sync.Once，Connect 首次失败后 err 永久缓存，
	// 后续 Take 永远返回同一 err 无法重试（漏洞4/liveness，不破互斥性——拿不到 client
	// 则 CAS 无法发起）。改为 mutex + initialized 标志：成功后缓存 client 不再重复 Connect；
	// 失败不缓存 err，下次 Take 可重试。
	initMu      sync.Mutex
	initialized bool
	client      *redis.Client
}

var _ storage.ConnectionManager[*redis.Client] = &RedisConnectionManager{}

// NewRedisConnectionManagerFromURI 从 Redis URI 创建连接管理器
func NewRedisConnectionManagerFromURI(uri string) *RedisConnectionManager {
	return &RedisConnectionManager{
		URI: uri,
	}
}

// NewRedisConnectionManagerFromOptions 从 Redis 选项创建连接管理器
func NewRedisConnectionManagerFromOptions(options *redis.Options) *RedisConnectionManager {
	return &RedisConnectionManager{
		Options: options,
	}
}

// NewRedisConnectionManagerFromClient 复用已经存在的 Redis 客户端，从其创建连接管理器
func NewRedisConnectionManagerFromClient(client *redis.Client) *RedisConnectionManager {
	return &RedisConnectionManager{
		client:      client,
		initialized: true, // 外部预设 client，Take 不再 Connect
	}
}

const RedisConnectionManagerName = "redis-connection-manager"

func (x *RedisConnectionManager) Name() string {
	return RedisConnectionManagerName
}

// Take 获取 Redis 客户端。
//
// 漏洞4 修复：原 sync.Once 在连接失败时永久缓存 err，无法重试。
// 改为 mutex + initialized：成功后缓存 client 不重复 Connect；失败不缓存 err 可重试。
func (x *RedisConnectionManager) Take(ctx context.Context) (*redis.Client, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	if x.initialized {
		return x.client, nil
	}

	var client *redis.Client
	if x.Options != nil {
		client = redis.NewClient(x.Options)
	} else if x.URI != "" {
		opts, err := redis.ParseURL(x.URI)
		if err != nil {
			// URI 解析失败不缓存，下次 Take 可重试
			return nil, err
		}
		client = redis.NewClient(opts)
	} else {
		// 既没有 Options 也没有 URI，无法创建连接
		return nil, ErrConnectionManagerNil
	}

	// 探活：确保连接可用
	if err := client.Ping(ctx).Err(); err != nil {
		// 连接失败不缓存，下次 Take 可重试
		return nil, err
	}

	x.client = client
	x.initialized = true
	return x.client, nil
}

func (x *RedisConnectionManager) Return(ctx context.Context, connection *redis.Client) error {
	return nil
}

func (x *RedisConnectionManager) Shutdown(ctx context.Context) error {
	// 连接池关闭的时候断掉连接，即使它是被复用的
	x.initMu.Lock()
	client := x.client
	x.initMu.Unlock()
	if client != nil {
		return client.Close()
	}
	return nil
}
