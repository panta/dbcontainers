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
	"os"
	"path/filepath"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	_ "github.com/go-sql-driver/mysql"
)

// MariaDBContainer implements the Container interface for MariaDB
type MariaDBContainer struct {
	dockerClient *client.Client
	containerID  string
	config       *Config
	hostPort     int
	tempDir      string
	useBindMount bool
	logger       *slog.Logger
}

// NewMariaDB creates a new MariaDB container instance
func NewMariaDB(logger *slog.Logger, config *Config) (*MariaDBContainer, error) {
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
			Image:         "mariadb:10.11",
			Port:          3306,
			Database:      "test",
			Username:      "root",
			Password:      "mariadb",
			Retry:         DefaultRetryConfig(),
			DockerHost:    dockerHost,
			SkipBindMount: false,
			TmpBase:       tmpBase,
		}
	}

	if config.Image == "" {
		config.Image = "mariadb:10.11"
	}
	if config.Port == 0 {
		config.Port = 3306
	}
	if config.Database == "" {
		config.Database = "test"
	}
	if config.Username == "" {
		config.Username = "root"
	}
	if config.Password == "" {
		config.Password = "mariadb"
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

	return &MariaDBContainer{
		dockerClient: cli,
		config:       config,
		tempDir:      tempDir,
		useBindMount: useBindMount,
		logger:       logger,
	}, nil
}

func (m *MariaDBContainer) Start(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, m.config.Retry.Timeout)
	defer cancel()

	if err := m.pullImage(ctx); err != nil {
		return err
	}

	if err := m.createAndStartContainer(ctx); err != nil {
		return err
	}

	return m.waitForReady(ctx)
}

func (m *MariaDBContainer) pullImage(ctx context.Context) error {
	m.logger.Debug(fmt.Sprintf("Pulling image: %s", m.config.Image), slog.String("image", m.config.Image))
	reader, err := m.dockerClient.ImagePull(ctx, m.config.Image, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull image: %w", err)
	}
	defer reader.Close()

	// Log pull progress
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
			m.logger.Error("Error decoding pull status", slog.Any("error", err))
			continue
		}
		if pullStatus.ID != "" {
			m.logger.Debug(fmt.Sprintf("[%s] %s %s", pullStatus.ID, pullStatus.Status, pullStatus.Progress))
		} else {
			m.logger.Debug(fmt.Sprintf("%s %s", pullStatus.Status, pullStatus.Progress))
		}
	}

	_, err = io.Copy(io.Discard, reader)
	return err
}

func (m *MariaDBContainer) createAndStartContainer(ctx context.Context) error {
	m.logger.Debug("Creating container...")

	var mounts []mount.Mount
	if m.useBindMount && m.tempDir != "" {
		mounts = []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: m.tempDir,
				Target: "/docker-entrypoint-initdb.d",
			},
		}
	}

	port := nat.Port(fmt.Sprintf("%d/tcp", m.config.Port))
	resp, err := m.dockerClient.ContainerCreate(ctx,
		&container.Config{
			Image: m.config.Image,
			Env: []string{
				fmt.Sprintf("MARIADB_DATABASE=%s", m.config.Database),
				fmt.Sprintf("MARIADB_USER=%s", m.config.Username),
				fmt.Sprintf("MARIADB_PASSWORD=%s", m.config.Password),
				fmt.Sprintf("MARIADB_ROOT_PASSWORD=%s", m.config.Password),
			},
			ExposedPorts: nat.PortSet{port: struct{}{}},
			Healthcheck: &container.HealthConfig{
				Test:     []string{"CMD-SHELL", fmt.Sprintf("mariadb-admin ping -h localhost -u%s -p%s", m.config.Username, m.config.Password)},
				Interval: time.Second * 1,
				Timeout:  time.Second * 3,
				Retries:  30,
			},
		},
		&container.HostConfig{
			PortBindings: nat.PortMap{
				port: []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: "0"}},
			},
			Mounts: mounts,
			Resources: container.Resources{
				Memory:     512 * 1024 * 1024,  // 512MB
				MemorySwap: 1024 * 1024 * 1024, // 1GB
				CPUShares:  1024,               // CPU priority
			},
			Privileged: true, // Needed for timer initialization
			Tmpfs: map[string]string{
				"/tmp":        "rw,noexec,nosuid,size=65536k",
				"/run/mysqld": "rw,noexec,nosuid,size=65536k",
			},
		},
		nil,
		nil,
		"",
	)
	if err != nil {
		return fmt.Errorf("failed to create container: %w", err)
	}

	m.containerID = resp.ID
	m.logger.Debug(fmt.Sprintf("Created container: %s", m.containerID[:12]))

	if err := m.mountInitScripts(ctx); err != nil {
		return fmt.Errorf("failed to create container SQL init scripts: %w", err)
	}

	m.logger.Debug("Starting container...")
	if err := m.dockerClient.ContainerStart(ctx, m.containerID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	if m.config.Debug {
		// Immediately get logs to see startup issues
		logReader, err := m.dockerClient.ContainerLogs(ctx, m.containerID, container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
		})
		if err != nil {
			m.logger.Warn("Warning: Failed to get container logs", slog.Any("error", err))
		} else {
			go func() {
				scanner := bufio.NewScanner(logReader)
				for scanner.Scan() {
					m.logger.Debug("container log", slog.String("log", scanner.Text()))
				}
				logReader.Close()
			}()
		}
	}

	inspect, err := m.dockerClient.ContainerInspect(ctx, m.containerID)
	if err != nil {
		return fmt.Errorf("failed to inspect container: %w", err)
	}

	bindings := inspect.NetworkSettings.Ports[port]
	if len(bindings) > 0 {
		m.hostPort = parseInt(bindings[0].HostPort)
		m.logger.Debug(fmt.Sprintf("Container port %d mapped to host port %d", m.config.Port, m.hostPort))
	}

	return nil
}

func (m *MariaDBContainer) mountInitScripts(ctx context.Context) error {
	if !m.useBindMount || m.tempDir == "" {
		return nil
	}

	for idx, initScriptContents := range m.config.InitScripts {
		scriptName := fmt.Sprintf("%03d-init-script.sql", idx)
		dstPath := filepath.Join(m.tempDir, scriptName)
		if err := os.WriteFile(dstPath, []byte(initScriptContents), 0644); err != nil {
			return fmt.Errorf("failed to write init script %d: %w", idx, err)
		}
	}
	return nil
}

func (m *MariaDBContainer) Stop(ctx context.Context) error {
	if m.containerID == "" {
		return nil
	}

	stopTimeout := 10 * time.Second
	stopTimeoutSecs := int(stopTimeout.Seconds())
	if err := m.dockerClient.ContainerStop(ctx, m.containerID, container.StopOptions{Timeout: &stopTimeoutSecs}); err != nil {
		return fmt.Errorf("failed to stop container: %w", err)
	}

	defer func() {
		if m.tempDir != "" {
			_ = os.RemoveAll(m.tempDir)
		}
	}()

	return m.dockerClient.ContainerRemove(ctx, m.containerID, container.RemoveOptions{
		RemoveVolumes: true,
		Force:         true,
	})
}

func (m *MariaDBContainer) Logs(ctx context.Context) ([]byte, error) {
	if m.containerID == "" {
		return nil, errors.New("container not started")
	}

	reader, err := m.dockerClient.ContainerLogs(ctx, m.containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get container logs: %w", err)
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

func (m *MariaDBContainer) RunSQL(ctx context.Context, script string) error {
	db, err := sql.Open("mysql", m.ConnectionString())
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, script); err != nil {
		return fmt.Errorf("failed to execute SQL script: %w", err)
	}

	return nil
}

func (m *MariaDBContainer) ConnectionString() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		m.config.Username,
		m.config.Password,
		m.config.DockerHost,
		m.hostPort,
		m.config.Database,
	)
}

func (m *MariaDBContainer) waitForReady(ctx context.Context) error {
	m.logger.Debug("Waiting for container to be ready...")
	ticker := time.NewTicker(m.config.Retry.Delay)
	defer ticker.Stop()

	for attempt := 0; attempt < m.config.Retry.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.checkHealth(ctx); err == nil {
				if err := m.checkConnection(ctx); err == nil {
					return m.runInitScripts(ctx)
				}
			}
		}
	}

	return fmt.Errorf("container failed to become ready after %d attempts", m.config.Retry.MaxAttempts)
}

func (m *MariaDBContainer) checkHealth(ctx context.Context) error {
	inspect, err := m.dockerClient.ContainerInspect(ctx, m.containerID)
	if err != nil {
		return fmt.Errorf("container inspect failed: %w", err)
	}

	if !inspect.State.Running {
		return errors.New("container is not running")
	}

	if inspect.State.Health == nil {
		return errors.New("container has no health status yet")
	}

	if inspect.State.Health != nil && inspect.State.Health.Status != "healthy" {
		return fmt.Errorf("container health check failed: %s", inspect.State.Health.Status)
	}

	return nil
}

func (m *MariaDBContainer) checkConnection(ctx context.Context) error {
	m.logger.Debug(fmt.Sprintf("Attempting database connection to %s", m.ConnectionString()))
	db, err := sql.Open("mysql", m.ConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()

	m.logger.Debug("Successfully connected to database")
	return db.PingContext(ctx)
}

func (m *MariaDBContainer) runInitScripts(ctx context.Context) error {
	if len(m.config.InitScripts) == 0 {
		return nil
	}
	if m.useBindMount && m.tempDir != "" {
		return nil
	}

	for _, script := range m.config.InitScripts {
		if err := m.RunSQL(ctx, script); err != nil {
			return fmt.Errorf("failed to run init script: %w", err)
		}
	}

	return nil
}
