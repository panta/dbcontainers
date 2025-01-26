package dbcontainers

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"
)

func getPostgresLogger() *slog.Logger {
	debug := false
	debugStr := os.Getenv("DBCONTAINER_DEBUG")
	if debugStr != "" {
		parsedDebug, _ := strconv.ParseBool(debugStr)
		debug = parsedDebug
	}

	var logger *slog.Logger
	var opts *slog.HandlerOptions = &slog.HandlerOptions{}
	if debug {
		opts = &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}
	} else {
		opts = &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}
	}
	logger = slog.New(slog.NewTextHandler(os.Stderr, opts)).With(slog.String("component", "postgres-container"))
	return logger
}

func TestPostgresContainer(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		scripts []string
	}{
		{
			name: "default config",
			config: &Config{
				Database: "test_db",
				Username: "test_user",
				Password: "test_pass",
			},
		},
		{
			name: "with init scripts",
			config: &Config{
				Database: "test_db",
				Username: "test_user",
				Password: "test_pass",
			},
			scripts: []string{
				"CREATE TABLE test (id SERIAL PRIMARY KEY, name TEXT);",
				"INSERT INTO test (name) VALUES ('test');",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			if tt.scripts != nil {
				tt.config.InitScripts = tt.scripts
			}

			container, err := NewPostgres(getPostgresLogger(), tt.config)
			if err != nil {
				t.Fatalf("Failed to create container: %v", err)
			}

			// Start container
			if err := container.Start(ctx); err != nil {
				t.Fatalf("Failed to start container: %v", err)
			}
			defer container.Stop(ctx)

			// Test logs
			logs, err := container.Logs(ctx)
			if err != nil {
				t.Fatalf("Failed to get container logs: %v", err)
			}
			if len(logs) == 0 {
				t.Error("Expected non-empty container logs")
			}

			// Test database connection
			db, err := sql.Open("postgres", container.ConnectionString())
			if err != nil {
				t.Fatalf("Failed to open database connection: %v", err)
			}
			defer db.Close()

			// Add timeout for connection test
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			if err := db.PingContext(ctx); err != nil {
				t.Fatalf("Failed to ping database: %v", err)
			}

			// Test init scripts if provided
			if tt.scripts != nil {
				var count int
				err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count)
				if err != nil {
					t.Fatalf("Failed to query test table: %v", err)
				}
				if count != 1 {
					t.Errorf("Expected 1 row in test table, got %d", count)
				}
			}

			// Test SQL execution
			if err := container.RunSQL(ctx, "SELECT 1"); err != nil {
				t.Fatalf("Failed to execute SQL: %v", err)
			}
		})
	}
}
