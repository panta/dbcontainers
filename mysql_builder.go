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

// MySQLBuilder implements a fluent interface for configuring MySQL containers
type MySQLBuilder struct {
	config *Config
	logger *slog.Logger
}

// NewMySQLBuilder creates a new MySQL container builder
func NewMySQLBuilder(logger *slog.Logger) *MySQLBuilder {
	if logger == nil {
		logger = slog.Default()
	}

	dockerHost := getDockerHost()
	if dockerHost == "" {
		dockerHost = "localhost"
	}

	return &MySQLBuilder{
		config: &Config{
			Image:         "mysql:8",
			Port:          3306,
			Database:      "test",
			Username:      "root",
			Password:      "mysql",
			Retry:         DefaultRetryConfig(),
			DockerHost:    dockerHost,
			SkipBindMount: false,
			Debug:         false,
		},
		logger: logger,
	}
}

// WithVersion sets the MySQL version
func (b *MySQLBuilder) WithVersion(version string) *MySQLBuilder {
	b.config.Image = fmt.Sprintf("mysql:%s", version)
	return b
}

// WithPort sets the container port
func (b *MySQLBuilder) WithPort(port int) *MySQLBuilder {
	b.config.Port = port
	return b
}

// WithCredentials sets the database credentials
func (b *MySQLBuilder) WithCredentials(username, password string) *MySQLBuilder {
	b.config.Username = username
	b.config.Password = password
	return b
}

// WithDatabase sets the database name
func (b *MySQLBuilder) WithDatabase(name string) *MySQLBuilder {
	b.config.Database = name
	return b
}

// WithDockerHost sets the docker machine IP/hostname.
func (b *MySQLBuilder) WithDockerHost(dockerHost string) *MySQLBuilder {
	b.config.DockerHost = dockerHost
	return b
}

// WithDebug enables debug logging
func (b *MySQLBuilder) WithDebug(debug bool) *MySQLBuilder {
	b.config.Debug = debug
	return b
}

func (b *MySQLBuilder) WithBindMound(enable bool) *MySQLBuilder {
	b.config.SkipBindMount = !enable
	return b
}

func (b *MySQLBuilder) WithTmpBase(tmpBase string) *MySQLBuilder {
	b.config.TmpBase = tmpBase
	return b
}

// WithInitScript adds a single initialization script
func (b *MySQLBuilder) WithInitScript(script string) *MySQLBuilder {
	b.config.InitScripts = append(b.config.InitScripts, script)
	return b
}

// WithInitScriptFile adds initialization scripts from a file
func (b *MySQLBuilder) WithInitScriptFile(filename string) *MySQLBuilder {
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
func (b *MySQLBuilder) WithInitScriptFiles(filenames []string) *MySQLBuilder {
	for _, filename := range filenames {
		b.WithInitScriptFile(filename)
	}
	return b
}

// WithInitScriptFS adds initialization scripts from an fs.FS using glob patterns
func (b *MySQLBuilder) WithInitScriptFS(fsys fs.FS, patterns ...string) *MySQLBuilder {
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
func (b *MySQLBuilder) WithRetryPolicy(attempts int, delay time.Duration, timeout time.Duration) *MySQLBuilder {
	b.config.Retry = &RetryConfig{
		MaxAttempts: attempts,
		Delay:       delay,
		Timeout:     timeout,
	}
	return b
}

// Build creates and returns a new MySQL container
func (b *MySQLBuilder) Build(ctx context.Context) (Container, error) {
	return NewMySQL(b.logger, b.config)
}
