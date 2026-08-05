package redis_storage

import (
	"context"
	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
	"os"
	"os/exec"
	"testing"
	"time"
)

var redisServerCmd *exec.Cmd

func TestMain(m *testing.M) {
	// 尝试使用环境变量中的 Redis URI
	envName := "STORAGE_LOCK_REDIS_URI"
	uri := os.Getenv(envName)
	if uri != "" {
		os.Exit(m.Run())
	}

	// 环境变量未设置，尝试启动临时 redis-server
	redisPort := "6399"
	redisServerCmd = exec.Command("redis-server",
		"--port", redisPort,
		"--save", "",
		"--appendonly", "no",
		"--daemonize", "no",
	)
	redisServerCmd.Stdout = os.Stderr
	redisServerCmd.Stderr = os.Stderr
	if err := redisServerCmd.Start(); err != nil {
		// 无法启动 redis-server，跳过测试
		os.Exit(0)
	}

	// 等待 redis-server 就绪
	uri = "redis://localhost:" + redisPort
	redisCliCmd := exec.Command("redis-cli", "-p", redisPort, "PING")
	for i := 0; i < 30; i++ {
		if output, err := redisCliCmd.Output(); err == nil && len(output) > 0 {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}

	os.Setenv(envName, uri)
	exitCode := m.Run()

	// 清理
	if redisServerCmd != nil && redisServerCmd.Process != nil {
		_ = redisServerCmd.Process.Kill()
		_ = redisServerCmd.Wait()
	}

	os.Exit(exitCode)
}

func TestRedisStorage(t *testing.T) {
	envName := "STORAGE_LOCK_REDIS_URI"
	uri := os.Getenv(envName)
	if uri == "" {
		t.Skip("STORAGE_LOCK_REDIS_URI not set and redis-server not available")
	}

	connectionManager := NewRedisConnectionManagerFromURI(uri)
	s, err := NewRedisStorage(context.Background(), &RedisStorageOptions{
		ConnectionManager: connectionManager,
		KeyPrefix:         storage_test_helper.TestTableName,
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}
