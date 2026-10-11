package redis_storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	storage_lock "github.com/storage-lock/go-storage-lock"
)

// TestRedisStorage_Watch_ReceivesNotificationOnLock 验证 Watch() 在其它进程/实例
// 对同一把锁执行 Lock 时能收到通知。
//
// 曾经的问题：CAS 的 Lua 脚本（create/update/delete）从未向 tryWatchWithPublish
// 订阅的频道 PUBLISH 过任何消息，导致该分支永远静默；而它又总是"成功"返回，
// 使得 tryWatchWithKeyevent 这个本该兜底的分支永远不会被走到。因此 Watch()
// 在默认配置下（未开启 notify-keyspace-events）实际上收不到任何通知。
// 修复方式：让 CAS 脚本在写入成功后向 watch 频道 PUBLISH 最新的 json，
// 使 tryWatchWithPublish 成为真正可用的主路径。
func TestRedisStorage_Watch_ReceivesNotificationOnLock(t *testing.T) {
	envName := "STORAGE_LOCK_REDIS_URI"
	uri := os.Getenv(envName)
	if uri == "" {
		t.Skip("STORAGE_LOCK_REDIS_URI not set and redis-server not available")
	}

	ctx := context.Background()
	connectionManager := NewRedisConnectionManagerFromURI(uri)
	opts := &RedisStorageOptions{
		ConnectionManager: connectionManager,
		KeyPrefix:         "watchcheck",
	}
	s, err := NewRedisStorage(ctx, opts)
	if !assert.NoError(t, err) {
		return
	}

	ch, err := s.Watch(ctx, "watch-test-lock")
	if !assert.NoError(t, err) {
		return
	}

	lock, err := storage_lock.NewStorageLock(s, "watch-test-lock")
	if !assert.NoError(t, err) {
		return
	}
	lockDone := make(chan struct{})
	go func() {
		defer close(lockDone)
		time.Sleep(200 * time.Millisecond)
		_ = lock.Lock(ctx, "owner-x")
	}()

	select {
	case info := <-ch:
		if !assert.NotNil(t, info) {
			return
		}
		assert.Equal(t, "watch-test-lock", info.LockId)
		assert.Equal(t, "owner-x", info.OwnerId)
	case <-time.After(2 * time.Second):
		t.Fatal("no notification received within 2s -- Watch is broken")
	}

	// 等待 Lock() 真正返回（看门狗等内部状态都已就绪）之后再 UnLock，
	// 避免与仍在运行的 Lock() goroutine 并发访问同一个 Event 产生数据竞争。
	<-lockDone
	_ = lock.UnLock(ctx, "owner-x")
}
