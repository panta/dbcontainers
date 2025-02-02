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

// MariaDBBuilder implements a fluent interface for configuring MariaDB containers
type MariaDBBuilder struct {
	config *Config
	logger *slog.Logger
}

// NewMariaDBBuiler creates a new MariaDB container builder
func NewMariaDBBuiler(logger *slog.Logger) *MariaDBBuilder {
	if logger == nil {
		logger = slog.Default()
	}

	dockerHost := getDockerHost()
	if dockerHost == "" {
		dockerHost = "localhost"
	}

	return &MariaDBBuilder{
		config: &Config{
			Image:         "mariadb:10.11",
			Port:          3306,
			Database:      "test",
			Username:      "root",
			Password:      "mariadb",
			Retry:         DefaultRetryConfig(),
			DockerHost:    dockerHost,
			SkipBindMount: false,
			Debug:         false,
		},
		logger: logger,
	}
}

// WithVersion sets the MariaDB version
func (b *MariaDBBuilder) WithVersion(version string) *MariaDBBuilder {
	b.config.Image = fmt.Sprintf("mariadb:%s", version)
	return b
}

// WithPort sets the container port
func (b *MariaDBBuilder) WithPort(port int) *MariaDBBuilder {
	b.config.Port = port
	return b
}

// WithCredentials sets the database credentials
func (b *MariaDBBuilder) WithCredentials(username, password string) *MariaDBBuilder {
	b.config.Username = username
	b.config.Password = password
	return b
}

// WithDatabase sets the database name
func (b *MariaDBBuilder) WithDatabase(name string) *MariaDBBuilder {
	b.config.Database = name
	return b
}

// WithDockerHost sets the docker machine IP/hostname.
func (b *MariaDBBuilder) WithDockerHost(dockerHost string) *MariaDBBuilder {
	b.config.DockerHost = dockerHost
	return b
}

// WithDebug enables debug logging
func (b *MariaDBBuilder) WithDebug(debug bool) *MariaDBBuilder {
	b.config.Debug = debug
	return b
}

func (b *MariaDBBuilder) WithBindMound(enable bool) *MariaDBBuilder {
	b.config.SkipBindMount = !enable
	return b
}

func (b *MariaDBBuilder) WithTmpBase(tmpBase string) *MariaDBBuilder {
	b.config.TmpBase = tmpBase
	return b
}

// WithInitScript adds a single initialization script
func (b *MariaDBBuilder) WithInitScript(script string) *MariaDBBuilder {
	b.config.InitScripts = append(b.config.InitScripts, script)
	return b
}

// WithInitScriptFile adds initialization scripts from a file
func (b *MariaDBBuilder) WithInitScriptFile(filename string) *MariaDBBuilder {
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
func (b *MariaDBBuilder) WithInitScriptFiles(filenames []string) *MariaDBBuilder {
	for _, filename := range filenames {
		b.WithInitScriptFile(filename)
	}
	return b
}

// WithInitScriptFS adds initialization scripts from an fs.FS using glob patterns
func (b *MariaDBBuilder) WithInitScriptFS(fsys fs.FS, patterns ...string) *MariaDBBuilder {
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
func (b *MariaDBBuilder) WithRetryPolicy(attempts int, delay time.Duration, timeout time.Duration) *MariaDBBuilder {
	b.config.Retry = &RetryConfig{
		MaxAttempts: attempts,
		Delay:       delay,
		Timeout:     timeout,
	}
	return b
}

// Build creates and returns a new MariaDB container
func (b *MariaDBBuilder) Build(ctx context.Context) (Container, error) {
	return NewMariaDB(b.logger, b.config)
}
