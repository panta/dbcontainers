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

// PostgresBuilder implements a fluent interface for configuring PostgreSQL containers
type PostgresBuilder struct {
	config *Config
	logger *slog.Logger
}

// NewPostgresBuiler creates a new PostgreSQL container builder
func NewPostgresBuiler(logger *slog.Logger) *PostgresBuilder {
	if logger == nil {
		logger = slog.Default()
	}

	dockerHost := getDockerHost()
	if dockerHost == "" {
		dockerHost = "localhost"
	}

	return &PostgresBuilder{
		config: &Config{
			Image:         "postgres:15",
			Port:          5432,
			Database:      "test",
			Username:      "postgres",
			Password:      "postgres",
			Retry:         DefaultRetryConfig(),
			DockerHost:    dockerHost,
			SkipBindMount: false,
			Debug:         false,
		},
		logger: logger,
	}
}

// WithVersion sets the PostgreSQL version
func (b *PostgresBuilder) WithVersion(version string) *PostgresBuilder {
	b.config.Image = fmt.Sprintf("postgres:%s", version)
	return b
}

// WithPort sets the container port
func (b *PostgresBuilder) WithPort(port int) *PostgresBuilder {
	b.config.Port = port
	return b
}

// WithCredentials sets the database credentials
func (b *PostgresBuilder) WithCredentials(username, password string) *PostgresBuilder {
	b.config.Username = username
	b.config.Password = password
	return b
}

// WithDatabase sets the database name
func (b *PostgresBuilder) WithDatabase(name string) *PostgresBuilder {
	b.config.Database = name
	return b
}

// WithDockerHost sets the docker machine IP/hostname.
func (b *PostgresBuilder) WithDockerHost(dockerHost string) *PostgresBuilder {
	b.config.DockerHost = dockerHost
	return b
}

// WithDebug enables debug logging
func (b *PostgresBuilder) WithDebug(debug bool) *PostgresBuilder {
	b.config.Debug = debug
	return b
}

func (b *PostgresBuilder) WithBindMound(enable bool) *PostgresBuilder {
	b.config.SkipBindMount = !enable
	return b
}

func (b *PostgresBuilder) WithTmpBase(tmpBase string) *PostgresBuilder {
	b.config.TmpBase = tmpBase
	return b
}

// WithInitScript adds a single initialization script
func (b *PostgresBuilder) WithInitScript(script string) *PostgresBuilder {
	b.config.InitScripts = append(b.config.InitScripts, script)
	return b
}

// WithInitScriptFile adds initialization scripts from a file
func (b *PostgresBuilder) WithInitScriptFile(filename string) *PostgresBuilder {
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
func (b *PostgresBuilder) WithInitScriptFiles(filenames []string) *PostgresBuilder {
	for _, filename := range filenames {
		b.WithInitScriptFile(filename)
	}
	return b
}

// WithInitScriptFS adds initialization scripts from an fs.FS using glob patterns
func (b *PostgresBuilder) WithInitScriptFS(fsys fs.FS, patterns ...string) *PostgresBuilder {
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
func (b *PostgresBuilder) WithRetryPolicy(attempts int, delay time.Duration, timeout time.Duration) *PostgresBuilder {
	b.config.Retry = &RetryConfig{
		MaxAttempts: attempts,
		Delay:       delay,
		Timeout:     timeout,
	}
	return b
}

// Build creates and returns a new PostgreSQL container
func (b *PostgresBuilder) Build(ctx context.Context) (Container, error) {
	return NewPostgres(b.logger, b.config)
}
