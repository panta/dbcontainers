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
	_ "github.com/lib/pq"
)

// PostgresContainer implements the Container interface for PostgreSQL
type PostgresContainer struct {
	dockerClient *client.Client
	containerID  string
	config       *Config
	hostPort     int
	tempDir      string
	useBindMount bool
	logger       *slog.Logger
}

// NewPostgres creates a new PostgreSQL container instance
func NewPostgres(logger *slog.Logger, config *Config) (*PostgresContainer, error) {
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
			Image:         "postgres:15-bullseye",
			Port:          5432,
			Database:      "test",
			Username:      "postgres",
			Password:      "postgres",
			Retry:         DefaultRetryConfig(),
			DockerHost:    dockerHost,
			SkipBindMount: false,
			TmpBase:       tmpBase,
		}
	}

	if config.Image == "" {
		config.Image = "postgres:15-bullseye"
	}
	if config.Port == 0 {
		config.Port = 5432
	}
	if config.Database == "" {
		config.Database = "test"
	}
	if config.Username == "" {
		config.Username = "postgres"
	}
	if config.Password == "" {
		config.Password = "postgres"
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

	return &PostgresContainer{
		dockerClient: cli,
		config:       config,
		tempDir:      tempDir,
		useBindMount: useBindMount,
		logger:       logger,
	}, nil
}

// Start initializes and runs the container
func (p *PostgresContainer) Start(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.config.Retry.Timeout)
	defer cancel()

	// Pull image if needed
	if err := p.pullImage(ctx); err != nil {
		return err
	}

	// Create and start container
	if err := p.createAndStartContainer(ctx); err != nil {
		return err
	}

	// Wait for database to be ready
	return p.waitForReady(ctx)
}

func (p *PostgresContainer) pullImage(ctx context.Context) error {
	p.logger.Debug(fmt.Sprintf("Pulling image: %s", p.config.Image), slog.String("image", p.config.Image))
	reader, err := p.dockerClient.ImagePull(ctx, p.config.Image, image.PullOptions{})
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
			p.logger.Error("Error decoding pull status", slog.Any("error", err))
			continue
		}
		if pullStatus.ID != "" {
			p.logger.Debug(fmt.Sprintf("[%s] %s %s", pullStatus.ID, pullStatus.Status, pullStatus.Progress))
		} else {
			p.logger.Debug(fmt.Sprintf("%s %s", pullStatus.Status, pullStatus.Progress))
		}
	}

	return nil
}

func (p *PostgresContainer) createAndStartContainer(ctx context.Context) error {
	p.logger.Debug("Creating container...")

	var mounts []mount.Mount
	if p.useBindMount && p.tempDir != "" {
		mounts = []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: p.tempDir,
				Target: "/docker-entrypoint-initdb.d",
			},
		}
	}

	port := nat.Port(fmt.Sprintf("%d/tcp", p.config.Port))
	resp, err := p.dockerClient.ContainerCreate(ctx,
		&container.Config{
			Image: p.config.Image,
			Env: []string{
				fmt.Sprintf("POSTGRES_DB=%s", p.config.Database),
				fmt.Sprintf("POSTGRES_USER=%s", p.config.Username),
				fmt.Sprintf("POSTGRES_PASSWORD=%s", p.config.Password),
				// "POSTGRES_HOST_AUTH_METHOD=trust", // For debugging
			},
			ExposedPorts: nat.PortSet{port: struct{}{}},
			Healthcheck: &container.HealthConfig{
				Test:     []string{"CMD-SHELL", fmt.Sprintf("pg_isready -U %s -d %s", p.config.Username, p.config.Database)},
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
			},
		},
		nil,
		nil,
		"",
	)
	if err != nil {
		return fmt.Errorf("failed to create container: %w", err)
	}

	p.containerID = resp.ID
	p.logger.Debug(fmt.Sprintf("Created container: %s", p.containerID[:12]))

	if err := p.mountInitScripts(ctx); err != nil {
		return fmt.Errorf("failed to create container SQL init scripts: %w", err)
	}

	p.logger.Debug("Starting container...")
	if err := p.dockerClient.ContainerStart(ctx, p.containerID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	// // Check if postgres binary exists
	// exec, err := p.dockerClient.ContainerExecCreate(ctx, p.containerID, container.ExecOptions{
	// 	Cmd:          []string{"ls", "-l", "/usr/lib/postgresql/15/bin/postgres"},
	// 	AttachStdout: true,
	// 	AttachStderr: true,
	// })
	// if err == nil {
	// 	resp, err := p.dockerClient.ContainerExecAttach(ctx, exec.ID, container.ExecAttachOptions{})
	// 	if err == nil {
	// 		defer resp.Close()
	// 		output, _ := io.ReadAll(resp.Reader)
	// 		p.logger.Debug(fmt.Sprintf("Postgres binary check: %s", string(output)))
	// 	}
	// }

	if p.config.Debug {
		// Immediately get logs to see startup issues
		logReader, err := p.dockerClient.ContainerLogs(ctx, p.containerID, container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
		})
		if err != nil {
			p.logger.Warn("Warning: Failed to get container logs", slog.Any("error", err))
		} else {
			go func() {
				scanner := bufio.NewScanner(logReader)
				for scanner.Scan() {
					p.logger.Debug("container log", slog.String("log", scanner.Text()))
				}
				logReader.Close()
			}()
		}
	}

	inspect, err := p.dockerClient.ContainerInspect(ctx, p.containerID)
	if err != nil {
		return fmt.Errorf("failed to inspect container: %w", err)
	}

	bindings := inspect.NetworkSettings.Ports[port]
	if len(bindings) > 0 {
		p.hostPort = parseInt(bindings[0].HostPort)
		p.logger.Debug(fmt.Sprintf("Container port %d mapped to host port %d", p.config.Port, p.hostPort))
	}

	return nil
}

func (p *PostgresContainer) mountInitScripts(ctx context.Context) error {
	if !p.useBindMount || p.tempDir == "" {
		return nil
	}

	for idx, initScriptContents := range p.config.InitScripts {
		scriptName := fmt.Sprintf("%03d-init-script.sql", idx)
		dstPath := filepath.Join(p.tempDir, scriptName)
		if err := os.WriteFile(dstPath, []byte(initScriptContents), 0644); err != nil {
			return fmt.Errorf("failed to write init script %d: %w", idx, err)
		}
	}
	return nil
}

// Stop terminates the container
func (p *PostgresContainer) Stop(ctx context.Context) error {
	if p.containerID == "" {
		return nil
	}

	stopTimeout := 10 * time.Second
	stopTimeoutSecs := int(stopTimeout.Seconds())
	if err := p.dockerClient.ContainerStop(ctx, p.containerID, container.StopOptions{Timeout: &stopTimeoutSecs}); err != nil {
		return fmt.Errorf("failed to stop container: %w", err)
	}

	defer func() {
		if p.tempDir != "" {
			_ = os.RemoveAll(p.tempDir)
		}
	}()

	return p.dockerClient.ContainerRemove(ctx, p.containerID, container.RemoveOptions{
		RemoveVolumes: true,
		Force:         true,
	})
}

// Logs returns container logs
func (p *PostgresContainer) Logs(ctx context.Context) ([]byte, error) {
	if p.containerID == "" {
		return nil, errors.New("container not started")
	}

	reader, err := p.dockerClient.ContainerLogs(ctx, p.containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get container logs: %w", err)
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// RunSQL executes SQL scripts against the database
func (p *PostgresContainer) RunSQL(ctx context.Context, script string) error {
	db, err := sql.Open("postgres", p.ConnectionString())
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, script); err != nil {
		return fmt.Errorf("failed to execute SQL script: %w", err)
	}

	return nil
}

// ConnectionString returns the database connection string
func (p *PostgresContainer) ConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		p.config.Username,
		p.config.Password,
		p.config.DockerHost,
		p.hostPort,
		p.config.Database,
	)
}

// waitForReady performs health checks until the database is ready
func (p *PostgresContainer) waitForReady(ctx context.Context) error {
	p.logger.Debug("Waiting for container to be ready...")
	ticker := time.NewTicker(p.config.Retry.Delay)
	defer ticker.Stop()

	for attempt := 0; attempt < p.config.Retry.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := p.checkHealth(ctx); err == nil {
				if err := p.checkConnection(ctx); err == nil {
					return p.runInitScripts(ctx)
				}
			}
		}
	}

	return fmt.Errorf("container failed to become ready after %d attempts", p.config.Retry.MaxAttempts)
}

func (p *PostgresContainer) checkHealth(ctx context.Context) error {
	inspect, err := p.dockerClient.ContainerInspect(ctx, p.containerID)
	if err != nil {
		return fmt.Errorf("container inspect failed: %w", err)
	}

	p.logger.Debug(fmt.Sprintf("Container state: %#v", inspect.State))

	if !inspect.State.Running {
		return errors.New("container is not running")
	}

	if inspect.State.Health == nil {
		p.logger.Debug(fmt.Sprintf("Container health check configuration: %#v", inspect.Config.Healthcheck))
		return errors.New("container has no health status yet")
	}

	p.logger.Debug(fmt.Sprintf("Health status: %s", inspect.State.Health.Status))

	// Log all health checks for debugging
	p.logger.Debug(fmt.Sprintf("Health check history (%d entries):", len(inspect.State.Health.Log)))
	for i, log := range inspect.State.Health.Log {
		p.logger.Debug(fmt.Sprintf("Health check #%d: Exit code %d, Output: %s, Start: %s, End: %s",
			i, log.ExitCode, log.Output, log.Start.Format(time.RFC3339), log.End.Format(time.RFC3339)))
	}

	if inspect.State.Health.Status != "healthy" {
		return fmt.Errorf("container health check failed: %s", inspect.State.Health.Status)
	}

	return nil
}

func (p *PostgresContainer) checkConnection(ctx context.Context) error {
	p.logger.Debug(fmt.Sprintf("Attempting database connection to %s", p.ConnectionString()))
	db, err := sql.Open("postgres", p.ConnectionString())
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	p.logger.Debug("Successfully connected to database")
	return nil
}

func (p *PostgresContainer) runInitScripts(ctx context.Context) error {
	if len(p.config.InitScripts) == 0 {
		return nil
	}
	if p.useBindMount && p.tempDir != "" {
		return nil
	}

	for _, script := range p.config.InitScripts {
		if err := p.RunSQL(ctx, script); err != nil {
			return fmt.Errorf("failed to run init script: %w", err)
		}
	}

	return nil
}
