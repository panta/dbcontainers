package dbcontainers

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"
)

// ClickHouseBuilder implements a fluent interface for configuring ClickHouse containers
type ClickHouseBuilder struct {
	config *Config
	logger *slog.Logger
}

// NewClickHouseBuilder creates a new ClickHouse container builder
func NewClickHouseBuilder(logger *slog.Logger) *ClickHouseBuilder {
	if logger == nil {
		logger = slog.Default()
	}

	dockerHost := getDockerHost()
	if dockerHost == "" {
		dockerHost = "localhost"
	}

	return &ClickHouseBuilder{
		config: &Config{
			Image:         "clickhouse/clickhouse-server:23.8",
			Port:          9000,
			Database:      "default",
			Username:      "default",
			Password:      "",
			Retry:         DefaultRetryConfig(),
			DockerHost:    dockerHost,
			SkipBindMount: false,
			Debug:         false,
		},
		logger: logger,
	}
}

// WithVersion sets the ClickHouse version
func (b *ClickHouseBuilder) WithVersion(version string) *ClickHouseBuilder {
	b.config.Image = fmt.Sprintf("clickhouse/clickhouse-server:%s", version)
	return b
}

// WithPort sets the container port
func (b *ClickHouseBuilder) WithPort(port int) *ClickHouseBuilder {
	b.config.Port = port
	return b
}

// WithCredentials sets the database credentials
func (b *ClickHouseBuilder) WithCredentials(username, password string) *ClickHouseBuilder {
	b.config.Username = username
	b.config.Password = password
	return b
}

// WithDatabase sets the database name
func (b *ClickHouseBuilder) WithDatabase(name string) *ClickHouseBuilder {
	b.config.Database = name
	return b
}

// WithDockerHost sets the docker machine IP/hostname
func (b *ClickHouseBuilder) WithDockerHost(dockerHost string) *ClickHouseBuilder {
	b.config.DockerHost = dockerHost
	return b
}

// WithDebug enables debug logging
func (b *ClickHouseBuilder) WithDebug(debug bool) *ClickHouseBuilder {
	b.config.Debug = debug
	return b
}

// WithBindMount controls whether to use bind mounting for init scripts
func (b *ClickHouseBuilder) WithBindMount(enable bool) *ClickHouseBuilder {
	b.config.SkipBindMount = !enable
	return b
}

// WithTmpBase sets the temporary directory base path
func (b *ClickHouseBuilder) WithTmpBase(tmpBase string) *ClickHouseBuilder {
	b.config.TmpBase = tmpBase
	return b
}

// WithInitScript adds a single initialization script
func (b *ClickHouseBuilder) WithInitScript(script string) *ClickHouseBuilder {
	b.config.InitScripts = append(b.config.InitScripts, script)
	return b
}

// WithInitScriptFile adds initialization scripts from a file
func (b *ClickHouseBuilder) WithInitScriptFile(filename string) *ClickHouseBuilder {
	content, err := os.ReadFile(filename)
	if err != nil {
		b.logger.Error("Failed to read init script file",
			slog.String("file", filename),
			slog.Any("error", err))
		return b
	}

	b.config.InitScripts = append(b.config.InitScripts, string(content))
	return b
}

// WithInitScriptFiles adds initialization scripts from multiple files
func (b *ClickHouseBuilder) WithInitScriptFiles(filenames []string) *ClickHouseBuilder {
	for _, filename := range filenames {
		b.WithInitScriptFile(filename)
	}
	return b
}

// WithInitScriptFS adds initialization scripts from an fs.FS using glob patterns
func (b *ClickHouseBuilder) WithInitScriptFS(fsys fs.FS, patterns ...string) *ClickHouseBuilder {
	for _, pattern := range patterns {
		matches, err := fs.Glob(fsys, pattern)
		if err != nil {
			b.logger.Error("Failed to glob pattern",
				slog.String("pattern", pattern),
				slog.Any("error", err))
			continue
		}

		// Sort matches to ensure consistent order
		sort.Strings(matches)

		for _, match := range matches {
			content, err := fs.ReadFile(fsys, match)
			if err != nil {
				b.logger.Error("Failed to read init script from FS",
					slog.String("file", match),
					slog.Any("error", err))
				continue
			}

			if strings.HasSuffix(match, ".sql") {
				b.config.InitScripts = append(b.config.InitScripts, string(content))
			}
		}
	}
	return b
}

// WithRetryPolicy sets the retry policy for container operations
func (b *ClickHouseBuilder) WithRetryPolicy(attempts int, delay time.Duration, timeout time.Duration) *ClickHouseBuilder {
	b.config.Retry = &RetryConfig{
		MaxAttempts: attempts,
		Delay:       delay,
		Timeout:     timeout,
	}
	return b
}

// Build creates and returns a new ClickHouse container
func (b *ClickHouseBuilder) Build(ctx context.Context) (Container, error) {
	return NewClickHouse(b.logger, b.config)
}
