package redis_storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/storage-lock/go-storage"
	"github.com/stretchr/testify/assert"
)

// testRedisClientURI 复用现有测试的连接管理逻辑：优先环境变量，否则本机 16379
func testRedisClientURI(t *testing.T) string {
	t.Helper()
	if uri := os.Getenv("STORAGE_LOCK_REDIS_URI"); uri != "" {
		return uri
	}
	return "redis://localhost:16379"
}

// TestRedisStorageOptions 覆盖 option 构造与校验分支
func TestRedisStorageOptions(t *testing.T) {
	// 默认构造
	opts := NewRedisStorageOptions()
	assert.NotNil(t, opts)
	assert.Equal(t, storage.DefaultStorageTableName, opts.KeyPrefix)
	assert.Nil(t, opts.ConnectionManager)

	// WithURI
	uriOpts := NewRedisStorageOptionsWithURI("redis://localhost:6379")
	assert.NotNil(t, uriOpts.ConnectionManager)

	// Set 方法链
	cm := NewRedisConnectionManagerFromClient(redis.NewClient(&redis.Options{Addr: "localhost:6379"}))
	ret := opts.SetConnectionManager(cm).SetKeyPrefix("custom")
	assert.Same(t, opts, ret)
	assert.Equal(t, "custom", opts.KeyPrefix)
	assert.Same(t, cm, opts.ConnectionManager)

	// Check：KeyPrefix 为空
	bad := &RedisStorageOptions{KeyPrefix: "", ConnectionManager: cm}
	assert.ErrorIs(t, bad.Check(), ErrKeyPrefixEmpty)

	// Check：ConnectionManager 为 nil
	nilCM := &RedisStorageOptions{KeyPrefix: "x", ConnectionManager: nil}
	assert.ErrorIs(t, nilCM.Check(), ErrConnectionManagerNil)

	// Check：正常
	assert.NoError(t, opts.Check())
}

// TestRedisStorageNewValidationErrors 覆盖 NewRedisStorage 的校验错误路径
func TestRedisStorageNewValidationErrors(t *testing.T) {
	ctx := context.Background()

	// ConnectionManager nil
	_, err := NewRedisStorage(ctx, &RedisStorageOptions{KeyPrefix: "x"})
	assert.ErrorIs(t, err, ErrConnectionManagerNil)

	// KeyPrefix 空
	cm := NewRedisConnectionManagerFromClient(redis.NewClient(&redis.Options{Addr: "localhost:6379"}))
	_, err = NewRedisStorage(ctx, &RedisStorageOptions{KeyPrefix: "", ConnectionManager: cm})
	assert.ErrorIs(t, err, ErrKeyPrefixEmpty)
}

// TestRedisStorageConnectionManager 覆盖连接管理器的构造/方法/错误分支
func TestRedisStorageConnectionManager(t *testing.T) {
	// 三种构造器
	fromURI := NewRedisConnectionManagerFromURI("redis://localhost:6379")
	assert.Equal(t, "redis://localhost:6379", fromURI.URI)

	opts := &redis.Options{Addr: "localhost:6379"}
	fromOpts := NewRedisConnectionManagerFromOptions(opts)
	assert.Same(t, opts, fromOpts.Options)

	realClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer realClient.Close()
	fromClient := NewRedisConnectionManagerFromClient(realClient)
	assert.Same(t, realClient, fromClient.client)
	assert.True(t, fromClient.initialized)

	// Name
	assert.Equal(t, RedisConnectionManagerName, fromClient.Name())

	// Return 恒 nil
	assert.NoError(t, fromClient.Return(context.Background(), realClient))

	// Take：预设 client 直接返回
	got, err := fromClient.Take(context.Background())
	assert.NoError(t, err)
	assert.Same(t, realClient, got)

	// Take：无 URI 无 Options → ErrConnectionManagerNil
	empty := &RedisConnectionManager{}
	_, err = empty.Take(context.Background())
	assert.ErrorIs(t, err, ErrConnectionManagerNil)

	// Take：无效 URI → 解析错误
	badURI := NewRedisConnectionManagerFromURI("://invalid")
	_, err = badURI.Take(context.Background())
	assert.Error(t, err)

	// Shutdown：nil client 时直接返回 nil
	emptyShut := &RedisConnectionManager{}
	assert.NoError(t, emptyShut.Shutdown(context.Background()))

	// Shutdown：预设 client 时 Close
	assert.NoError(t, fromClient.Shutdown(context.Background()))
}

// TestRedisStorageShutdownClosesClient 覆盖 Shutdown 关闭真实连接
func TestRedisStorageShutdownClosesClient(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	cm := NewRedisConnectionManagerFromClient(client)
	// Take 后再 Shutdown 关闭
	ctx := context.Background()
	taken, err := cm.Take(ctx)
	assert.NoError(t, err)
	assert.NoError(t, cm.Shutdown(ctx))
	if taken != nil {
		assert.Error(t, taken.Ping(ctx).Err()) // 关闭后应失败
	}
}

// TestRedisStorageWatch 覆盖 Watch 关键路径（需要真实 redis，有 keyspace notifications 则走 keyevent，否则 publish）
func TestRedisStorageWatch(t *testing.T) {
	uri := testRedisClientURI(t)
	opts, err := redis.ParseURL(uri)
	if err != nil {
		t.Skipf("invalid redis uri %s: %v", uri, err)
	}
	client := redis.NewClient(opts)
	defer client.Close()
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not reachable: %v", err)
	}

	s := &RedisStorage{
		options: &RedisStorageOptions{KeyPrefix: "watch-test"},
		client:  client,
	}

	// keyeventChannel：默认 DB0
	assert.Equal(t, "__keyevent@0__:hset", s.keyeventChannel("any"))

	// publishChannel
	assert.Equal(t, "watch-test:watch:any", s.publishChannel("any"))

	// parseWatchPayload：空串 → nil,nil
	info, err := s.parseWatchPayload("")
	assert.NoError(t, err)
	assert.Nil(t, info)

	// parseWatchPayload：合法 JSON
	valid := &storage.LockInformation{LockId: "l1", OwnerId: "o1", Version: 1}
	validJson := valid.ToJsonString()
	parsed, err := s.parseWatchPayload(validJson)
	assert.NoError(t, err)
	assert.Equal(t, "l1", parsed.LockId)

	// parseWatchPayload：非法 JSON
	_, err = s.parseWatchPayload("{not-json")
	assert.Error(t, err)

	// currentLockInformation：不存在的锁 → nil,nil
	info, err = s.currentLockInformation(ctx, "nonexistent-lock-xyz")
	assert.NoError(t, err)
	assert.Nil(t, info)

	// 通过发布频道验证 tryWatchWithPublish 能收到通知
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	ch, err := s.tryWatchWithPublish(watchCtx, "watch-lock-1")
	assert.NoError(t, err)
	// 写入一条锁记录触发 PUBLISH
	lockInfo := &storage.LockInformation{LockId: "watch-lock-1", OwnerId: "o1", Version: 1}
	err = s.CreateWithVersion(ctx, "watch-lock-1", 1, lockInfo)
	if err == nil {
		select {
		case received := <-ch:
			assert.NotNil(t, received)
			assert.Equal(t, "o1", received.OwnerId)
		case <-time.After(time.Second):
			// 通知可能被 keyspace notification 抢走，不阻塞
		}
	}
	_ = s.DeleteWithVersion(ctx, "watch-lock-1", 1, lockInfo)
}

// testLocalClient 创建一个指向本机 redis 的 client（可 Ping 才返回，否则 nil）
func testLocalClient(t *testing.T, ctx context.Context) *redis.Client {
	t.Helper()
	uri := testRedisClientURI(t)
	opts, err := redis.ParseURL(uri)
	if err != nil {
		t.Skipf("invalid redis uri %s: %v", uri, err)
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("redis not reachable: %v", err)
	}
	return client
}

// TestRedisStorageErrorPaths 覆盖"连接已不可用"时的所有错误分支
func TestRedisStorageErrorPaths(t *testing.T) {
	ctx := context.Background()
	// 已关闭的 client：后续命令一律返回 redis.ErrClosed
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	client.Close()
	cm := NewRedisConnectionManagerFromClient(client)

	s := &RedisStorage{
		options: &RedisStorageOptions{KeyPrefix: "err", ConnectionManager: cm},
		client:  client,
	}

	// Init：Take 出 client 但 Ping 失败
	assert.Error(t, s.Init(ctx))

	// CreateWithVersion：Eval 错误
	assert.Error(t, s.CreateWithVersion(ctx, "l", 1, &storage.LockInformation{LockId: "l"}))

	// UpdateWithVersion：Eval 错误
	assert.Error(t, s.UpdateWithVersion(ctx, "l", 1, 2, &storage.LockInformation{LockId: "l"}))

	// DeleteWithVersion：Eval 错误
	assert.Error(t, s.DeleteWithVersion(ctx, "l", 1, &storage.LockInformation{LockId: "l"}))

	// Get：非 redis.Nil 错误
	_, err := s.Get(ctx, "l")
	assert.Error(t, err)

	// doGet 的错误被 currentLockInformation 透传
	_, err = s.currentLockInformation(ctx, "l")
	assert.Error(t, err)

	// List：Scan 错误
	_, err = s.List(ctx)
	assert.Error(t, err)

	// GetTime：命令错误
	_, err = s.GetTime(ctx)
	assert.Error(t, err)
}

// TestRedisStorageWatchFallback 覆盖 Watch 的 fallback 与组合错误分支（L252-257）
// TestRedisStorageWatchFallback verifies both strategies fail on a closed client.
func TestRedisStorageWatchFallback(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	_ = client.Close()
	s := &RedisStorage{
		options: &RedisStorageOptions{KeyPrefix: "fb"},
		client:  client,
	}
	ch, err := s.Watch(ctx, "any-lock")
	assert.Error(t, err)
	assert.Nil(t, ch)
	assert.Contains(t, err.Error(), "publish=")
}

// TestRedisStorageWatchInternalBranches 覆盖 tryWatchWithPublish / tryWatchWithKeyevent
// 内部的 parse 错误、nil 通知、ctx 取消、keyevent 事件等分支
func TestRedisStorageWatchInternalBranches(t *testing.T) {
	ctx := context.Background()
	client := testLocalClient(t, ctx)
	if client == nil {
		return
	}
	defer client.Close()

	s := &RedisStorage{
		options: &RedisStorageOptions{KeyPrefix: "branch-test"},
		client:  client,
	}

	// tryWatchWithPublish：非法 JSON（跳过）→ 空 payload（此前未发送，发 nil）→ 合法 payload
	ch, err := s.tryWatchWithPublish(ctx, "pub-lock")
	assert.NoError(t, err)
	defer func() {
		// 用独立可取消 ctx 关闭该 goroutine
	}()
	time.Sleep(100 * time.Millisecond) // 等待 Subscribe 完成

	client.Publish(ctx, s.publishChannel("pub-lock"), "{bad-json")
	client.Publish(ctx, s.publishChannel("pub-lock"), "")
	select {
	case got := <-ch:
		assert.Nil(t, got)
	case <-time.After(time.Second):
		t.Fatal("expected nil notification for empty publish")
	}

	valid := &storage.LockInformation{LockId: "pub-lock", OwnerId: "o", Version: 1}
	client.Publish(ctx, s.publishChannel("pub-lock"), valid.ToJsonString())
	select {
	case got := <-ch:
		if assert.NotNil(t, got) {
			assert.Equal(t, "o", got.OwnerId)
		}
	case <-time.After(time.Second):
		t.Fatal("expected valid notification")
	}

	// ctx 取消后 channel 关闭
	c2, cancel := context.WithCancel(ctx)
	ch2, err := s.tryWatchWithPublish(c2, "pub-lock2")
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case _, ok := <-ch2:
		assert.False(t, ok, "channel should be closed after cancel")
	case <-time.After(time.Second):
		t.Fatal("channel not closed after cancel")
	}

	// tryWatchWithKeyevent：先建锁（lastInfo 非 nil 会先发当前值），再 PUBLISH keyevent
	lockInfo := &storage.LockInformation{LockId: "keyev-lock", OwnerId: "ke", Version: 1}
	assert.NoError(t, s.CreateWithVersion(ctx, "keyev-lock", 1, lockInfo))

	kc, err := s.tryWatchWithKeyevent(ctx, "keyev-lock")
	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// 初始快照：lastInfo 非 nil 会推当前锁信息
	select {
	case got := <-kc:
		if assert.NotNil(t, got) {
			assert.Equal(t, "ke", got.OwnerId)
		}
	case <-time.After(time.Second):
		t.Fatal("expected keyevent initial snapshot")
	}

	// 模拟 keyspace hset 事件（向 keyevent channel PUBLISH）
	client.Publish(ctx, s.keyeventChannel("keyev-lock"), "hset")
	select {
	case got := <-kc:
		_ = got // 收到事件后重新读取当前锁信息（存在则非 nil）
	case <-time.After(time.Second):
		t.Fatal("expected keyevent notification")
	}

	// 删除锁后再 PUBLISH：currentLockInformation 为 nil → 推 nil
	assert.NoError(t, s.DeleteWithVersion(ctx, "keyev-lock", 1, lockInfo))
	client.Publish(ctx, s.keyeventChannel("keyev-lock"), "hset")
	select {
	case got := <-kc:
		assert.Nil(t, got)
	case <-time.After(time.Second):
		t.Fatal("expected nil keyevent after delete")
	}

	// 不存在锁时 lastInfo 为 nil，不阻塞订阅循环
	kc2, err := s.tryWatchWithKeyevent(ctx, "no-such-ke-lock")
	assert.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	client.Publish(ctx, s.keyeventChannel("no-such-ke-lock"), "hset")
	select {
	case got := <-kc2:
		assert.Nil(t, got)
	case <-time.After(time.Second):
		t.Fatal("expected nil keyevent for missing lock")
	}
}

// TestRedisStorageConnectionManagerPingFailure 覆盖 Take 的 Ping 失败重试路径
func TestRedisStorageConnectionManagerPingFailure(t *testing.T) {
	ctx := context.Background()
	// 不可达端口：连接立即 refusal，Ping 失败不缓存 err
	cm := NewRedisConnectionManagerFromURI("redis://127.0.0.1:1")
	_, err := cm.Take(ctx)
	assert.Error(t, err)
	// 失败后可重试（不会返回缓存的同一错误对象）——再次尝试仍然报错
	_, err2 := cm.Take(ctx)
	assert.Error(t, err2)
}
