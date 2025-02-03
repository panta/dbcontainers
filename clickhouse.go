package dbcontainers

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/docker/docker/api/types"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/docker/go-units"
)

// ClickHouseContainer implements the Container interface for ClickHouse
type ClickHouseContainer struct {
	dockerClient *client.Client
	containerID  string
	config       *Config
	hostPort     int
	tempDir      string
	useBindMount bool
	logger       *slog.Logger
}

// NewClickHouse creates a new ClickHouse container instance
func NewClickHouse(logger *slog.Logger, config *Config) (*ClickHouseContainer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	dockerHost := getDockerHost()
	if dockerHost == "" {
		dockerHost = "localhost"
	}

	tmpBase := ""
	if dockerHost != "localhost" && dockerHost != "127.0.0.1" {
		tmpBase = "."
	}

	if config == nil {
		config = &Config{
			Image:         "clickhouse/clickhouse-server:23.8",
			Port:          9000,
			Database:      "default",
			Username:      "default",
			Password:      "",
			Retry:         DefaultRetryConfig(),
			DockerHost:    dockerHost,
			SkipBindMount: false,
			TmpBase:       tmpBase,
		}
	}

	if config.Image == "" {
		config.Image = "clickhouse/clickhouse-server:23.8"
	}
	if config.Port == 0 {
		config.Port = 9000
	}
	if config.Database == "" {
		config.Database = "default"
	}
	if config.Username == "" {
		config.Username = "default"
	}
	if config.Retry == nil {
		config.Retry = DefaultRetryConfig()
	}
	if config.DockerHost == "" {
		config.DockerHost = dockerHost
	}
	if config.TmpBase == "" {
		config.TmpBase = tmpBase
	}

	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	useBindMount := !config.SkipBindMount
	var tempDir string
	if useBindMount {
		tempDir, err = os.MkdirTemp(".", "dbcontainers-tmp")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp dir: %w", err)
		}
		tempDir, err = filepath.Abs(tempDir)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path for temp dir: %w", err)
		}
	}

	return &ClickHouseContainer{
		dockerClient: cli,
		config:       config,
		tempDir:      tempDir,
		useBindMount: useBindMount,
		logger:       logger,
	}, nil
}

func (c *ClickHouseContainer) Start(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.config.Retry.Timeout)
	defer cancel()

	if err := c.pullImage(ctx); err != nil {
		return err
	}

	if err := c.createAndStartContainer(ctx); err != nil {
		_ = c.cleanup(ctx)
		return err
	}

	return c.waitForReady(ctx)
}

func (c *ClickHouseContainer) pullImage(ctx context.Context) error {
	c.logger.Debug(fmt.Sprintf("Pulling image: %s", c.config.Image))
	reader, err := c.dockerClient.ImagePull(ctx, c.config.Image, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull image: %w", err)
	}
	defer reader.Close()

	decoder := json.NewDecoder(reader)
	for {
		var pullStatus struct {
			Status   string `json:"status"`
			Progress string `json:"progress,omitempty"`
			ID       string `json:"id,omitempty"`
		}
		if err := decoder.Decode(&pullStatus); err != nil {
			if err == io.EOF {
				break
			}
			c.logger.Error("Error decoding pull status", slog.Any("error", err))
			continue
		}
		if pullStatus.ID != "" {
			c.logger.Debug(fmt.Sprintf("[%s] %s %s", pullStatus.ID, pullStatus.Status, pullStatus.Progress))
		} else {
			c.logger.Debug(fmt.Sprintf("%s %s", pullStatus.Status, pullStatus.Progress))
		}
	}

	return nil
}

func (c *ClickHouseContainer) createAndStartContainer(ctx context.Context) error {
	c.logger.Debug("Creating container...")

	var mounts []mount.Mount
	if c.useBindMount && c.tempDir != "" {
		mounts = []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: c.tempDir,
				Target: "/docker-entrypoint-initdb.d",
			},
		}
	}

	port := nat.Port(fmt.Sprintf("%d/tcp", c.config.Port))
	httpPort := nat.Port("8123/tcp") // ClickHouse HTTP interface

	env := []string{
		fmt.Sprintf("CLICKHOUSE_DB=%s", c.config.Database),
		fmt.Sprintf("CLICKHOUSE_USER=%s", c.config.Username),
		fmt.Sprintf("CLICKHOUSE_PASSWORD=%s", c.config.Password),
		fmt.Sprintf("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1"),
		"CLICKHOUSE_INIT_TIMEOUT=30",
		"CLICKHOUSE_START_TIMEOUT=30",
	}
	if c.config.Password != "" {
		env = append(env, fmt.Sprintf("CLICKHOUSE_PASSWORD=%s", c.config.Password))
	}

	resp, err := c.dockerClient.ContainerCreate(ctx,
		&container.Config{
			Image: c.config.Image,
			Env:   env,
			ExposedPorts: nat.PortSet{
				port:     struct{}{},
				httpPort: struct{}{},
			},
			Healthcheck: &container.HealthConfig{
				Test:     []string{"CMD-SHELL", "/usr/bin/wget --no-verbose --tries=1 --spider http://localhost:8123/ping || exit 1"},
				Interval: time.Second * 2,
				Timeout:  time.Second * 5,
				Retries:  30,
			},
		},
		&container.HostConfig{
			PortBindings: nat.PortMap{
				port:     []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: "0"}},
				httpPort: []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: "0"}},
			},
			Mounts: mounts,
			Resources: container.Resources{
				Memory:     1024 * 1024 * 1024, // 1GB
				MemorySwap: 2048 * 1024 * 1024, // 2GB
				Ulimits: []*units.Ulimit{
					{
						Name: "nofile",
						Soft: 262144,
						Hard: 262144,
					},
				},
			},
		},
		nil,
		nil,
		"",
	)
	if err != nil {
		return fmt.Errorf("failed to create container: %w", err)
	}

	c.containerID = resp.ID
	c.logger.Debug(fmt.Sprintf("Created container: %s", c.containerID[:12]))

	if err := c.mountInitScripts(ctx); err != nil {
		return fmt.Errorf("failed to create container SQL init scripts: %w", err)
	}

	c.logger.Debug("Starting container...")
	if err := c.dockerClient.ContainerStart(ctx, c.containerID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	if c.config.Debug {
		logReader, err := c.dockerClient.ContainerLogs(ctx, c.containerID, container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
		})
		if err != nil {
			c.logger.Warn("Warning: Failed to get container logs", slog.Any("error", err))
		} else {
			go func() {
				scanner := bufio.NewScanner(logReader)
				for scanner.Scan() {
					c.logger.Debug("container log", slog.String("log", scanner.Text()))
				}
				logReader.Close()
			}()
		}
	}

	inspect, err := c.dockerClient.ContainerInspect(ctx, c.containerID)
	if err != nil {
		return fmt.Errorf("failed to inspect container: %w", err)
	}

	bindings := inspect.NetworkSettings.Ports[port]
	if len(bindings) > 0 {
		c.hostPort = parseInt(bindings[0].HostPort)
		c.logger.Debug(fmt.Sprintf("Container port %d mapped to host port %d", c.config.Port, c.hostPort))
	}

	return nil
}

func (c *ClickHouseContainer) mountInitScripts(ctx context.Context) error {
	if !c.useBindMount || c.tempDir == "" {
		return nil
	}

	for idx, initScriptContents := range c.config.InitScripts {
		scriptName := fmt.Sprintf("%03d-init-script.sql", idx)
		dstPath := filepath.Join(c.tempDir, scriptName)
		if err := os.WriteFile(dstPath, []byte(initScriptContents), 0644); err != nil {
			return fmt.Errorf("failed to write init script %d: %w", idx, err)
		}
	}
	return nil
}

func (c *ClickHouseContainer) cleanup(ctx context.Context) error {
	if c.tempDir != "" {
		_ = os.RemoveAll(c.tempDir)
	}

	if c.containerID != "" {
		return c.dockerClient.ContainerRemove(ctx, c.containerID, container.RemoveOptions{
			RemoveVolumes: true,
			Force:         true,
		})
	}

	return nil
}

func (c *ClickHouseContainer) Stop(ctx context.Context) error {
	if c.containerID == "" {
		return nil
	}

	stopTimeout := 10 * time.Second
	stopTimeoutSecs := int(stopTimeout.Seconds())
	if err := c.dockerClient.ContainerStop(ctx, c.containerID, container.StopOptions{Timeout: &stopTimeoutSecs}); err != nil {
		return fmt.Errorf("failed to stop container: %w", err)
	}

	return c.cleanup(ctx)
}

func (c *ClickHouseContainer) Logs(ctx context.Context) ([]byte, error) {
	if c.containerID == "" {
		return nil, errors.New("container not started")
	}

	reader, err := c.dockerClient.ContainerLogs(ctx, c.containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get container logs: %w", err)
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

func (c *ClickHouseContainer) RunSQL(ctx context.Context, script string) error {
	db, err := sql.Open("clickhouse", c.ConnectionString())
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, script); err != nil {
		return fmt.Errorf("failed to execute SQL script: %w", err)
	}

	return nil
}

func (c *ClickHouseContainer) ConnectionString() string {
	// ClickHouse connection string format
	auth := c.config.Username
	if c.config.Password != "" {
		auth += ":" + c.config.Password
	}

	return fmt.Sprintf("clickhouse://%s@%s:%d/%s",
		auth,
		c.config.DockerHost,
		c.hostPort,
		c.config.Database,
	)
}

func (c *ClickHouseContainer) waitForReady(ctx context.Context) error {
	c.logger.Debug("Waiting for container to be ready...")

	// Initial delay to let the server start
	time.Sleep(time.Second * 5)

	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()

	successfulChecks := 0
	requiredSuccessfulChecks := 2 // Require multiple successful checks in a row

	failureCount := 0
	maxFailures := 10

	for attempt := 0; attempt < c.config.Retry.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.checkHealth(ctx); err == nil {
				successfulChecks++
				failureCount = 0 // Reset failure count on success
				c.logger.Debug(fmt.Sprintf("Successful health check %d/%d",
					successfulChecks, requiredSuccessfulChecks))

				if successfulChecks >= requiredSuccessfulChecks {
					c.logger.Debug("Container is ready and stable")
					return c.runInitScripts(ctx)
				}
			} else {
				successfulChecks = 0 // Reset on failure
				failureCount++
				c.logger.Debug(fmt.Sprintf("Health check failed (%d/%d): %v",
					failureCount, maxFailures, err))

				if failureCount >= maxFailures {
					return fmt.Errorf("too many consecutive failures: %v", err)
				}

				// On failure, check if container is still running
				inspect, inspectErr := c.dockerClient.ContainerInspect(ctx, c.containerID)
				if inspectErr != nil {
					c.logger.Debug("Container inspect failed",
						slog.Any("error", inspectErr))
					continue
				}

				if !inspect.State.Running {
					logs, _ := c.Logs(ctx)
					c.logger.Debug("Container stopped unexpectedly",
						slog.String("logs", string(logs)))
					return errors.New("container stopped unexpectedly")
				}
			}
		}
	}

	return fmt.Errorf("container failed to become ready after %d attempts",
		c.config.Retry.MaxAttempts)
}

func (c *ClickHouseContainer) checkHealth(ctx context.Context) error {
	inspect, err := c.dockerClient.ContainerInspect(ctx, c.containerID)
	if err != nil {
		return fmt.Errorf("container inspect failed: %w", err)
	}

	if !inspect.State.Running {
		c.logger.Debug("Container state details",
			slog.String("state", fmt.Sprintf("%+v", inspect.State)))
		return errors.New("container is not running")
	}

	// Try HTTP ping first
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
	}
	pingURL := fmt.Sprintf("http://%s:%d/ping",
		c.config.DockerHost,
		c.getHTTPPort(inspect))

	resp, err := httpClient.Get(pingURL)
	if err != nil {
		c.logger.Debug("HTTP ping failed", slog.Any("error", err))
		return fmt.Errorf("HTTP ping failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP ping returned status %d", resp.StatusCode)
	}
	c.logger.Debug("HTTP ping successful")

	// Verify native protocol
	db, err := sql.Open("clickhouse", c.ConnectionString())
	if err != nil {
		c.logger.Debug("Native connection open failed", slog.Any("error", err))
		return fmt.Errorf("failed to open native connection: %w", err)
	}
	defer db.Close()

	// Set a short timeout for the connection test
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		c.logger.Debug("Native protocol ping failed", slog.Any("error", err))
		return fmt.Errorf("native protocol ping failed: %w", err)
	}

	// Try a simple query to verify full readiness
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	row := db.QueryRowContext(queryCtx, "SELECT 1")
	var result int
	if err := row.Scan(&result); err != nil {
		c.logger.Debug("Test query failed", slog.Any("error", err))
		return fmt.Errorf("test query failed: %w", err)
	}

	c.logger.Debug("All health checks passed")
	return nil
}

func (c *ClickHouseContainer) getHTTPPort(inspect types.ContainerJSON) int {
	httpPort := nat.Port("8123/tcp")
	bindings := inspect.NetworkSettings.Ports[httpPort]
	if len(bindings) > 0 {
		if port, err := strconv.Atoi(bindings[0].HostPort); err == nil {
			return port
		}
	}
	return 8123 // fallback to default
}

func (c *ClickHouseContainer) checkNativeConnection(ctx context.Context) error {
	db, err := sql.Open("clickhouse", c.ConnectionString())
	if err != nil {
		c.logger.Debug("Failed to open native connection", slog.Any("error", err))
		return fmt.Errorf("failed to open connection: %w", err)
	}
	defer db.Close()

	// Set a longer timeout for the connection test
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		c.logger.Debug("Failed to ping database", slog.Any("error", err))
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Try a simple query to verify full readiness
	var result int
	row := db.QueryRowContext(ctx, "SELECT 1")
	if err := row.Scan(&result); err != nil {
		c.logger.Debug("Failed to execute test query", slog.Any("error", err))
		return fmt.Errorf("failed to execute test query: %w", err)
	}

	c.logger.Debug("Native connection test successful")
	return nil
}

func (c *ClickHouseContainer) checkConnection(ctx context.Context) error {
	db, err := sql.Open("clickhouse", c.ConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()

	return db.PingContext(ctx)
}

func (c *ClickHouseContainer) runInitScripts(ctx context.Context) error {
	if len(c.config.InitScripts) == 0 {
		return nil
	}
	if c.useBindMount && c.tempDir != "" {
		return nil
	}

	for _, script := range c.config.InitScripts {
		if err := c.RunSQL(ctx, script); err != nil {
			return fmt.Errorf("failed to run init script: %w", err)
		}
	}

	return nil
}
