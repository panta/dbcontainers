// Package dbcontainers is the main access point for the library functionality.
package dbcontainers

import (
	"context"
	"net/url"
	"os"
	"time"
)

// Container defines the interface for database containers
type Container interface {
	// Start initializes and runs the container
	Start(ctx context.Context) error

	// Stop terminates the container
	Stop(ctx context.Context) error

	// ConnectionString returns the database connection string
	ConnectionString() string

	// Logs returns container logs for debugging
	Logs(ctx context.Context) ([]byte, error)

	// RunSQL executes SQL scripts against the database
	RunSQL(ctx context.Context, script string) error
}

// Config holds common container configuration
type Config struct {
	Image         string
	Port          int
	Database      string
	Username      string
	Password      string
	InitScripts   []string
	Retry         *RetryConfig
	DockerHost    string
	SkipBindMount bool
	TmpBase       string
	Debug         bool
}

// RetryConfig defines retry behavior for operations
type RetryConfig struct {
	MaxAttempts int
	Delay       time.Duration
	Timeout     time.Duration
}

// DefaultRetryConfig returns sensible defaults for retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts: 60,
		Delay:       time.Second,
		Timeout:     time.Minute * 2,
	}
}

// getDockerHost returns the docker-machine IP or hostname based on env variables.
func getDockerHost() string {
	getHostName := func() string {
		dockerHostName := os.Getenv("DOCKER_MACHINE_NAME")
		if dockerHostName == "" {
			dockerHostName = "localhost"
		}
		return dockerHostName
	}

	dockerHost := os.Getenv("DOCKER_HOST")
	if dockerHost == "" {
		return getHostName()
	}

	// Parse the DOCKER_HOST value as a URL
	parsedURL, err := url.Parse(dockerHost)
	if err != nil {
		return getHostName()
	}

	return parsedURL.Hostname()
}
