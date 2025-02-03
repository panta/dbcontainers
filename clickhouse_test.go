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

func getClickHouseLogger() *slog.Logger {
	debug := false
	debugStr := os.Getenv("DBCONTAINER_DEBUG")
	if debugStr != "" {
		parsedDebug, _ := strconv.ParseBool(debugStr)
		debug = parsedDebug
	}

	var opts *slog.HandlerOptions = &slog.HandlerOptions{}
	if debug {
		opts.Level = slog.LevelDebug
	} else {
		opts.Level = slog.LevelInfo
	}

	return slog.New(slog.NewTextHandler(os.Stderr, opts)).With(slog.String("component", "clickhouse-container"))
}

func TestClickHouseContainer(t *testing.T) {
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
				Debug:    true,
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
				`CREATE DATABASE IF NOT EXISTS test_db`,
				`CREATE TABLE IF NOT EXISTS test_db.test (
					id UInt32,
					name String
				) ENGINE = MergeTree()
				ORDER BY id`,
				"INSERT INTO test_db.test (id, name) VALUES (1, 'test')",
			},
		},
		{
			name: "with complex schema",
			config: &Config{
				Database: "test_db",
				Username: "test_user",
				Password: "test_pass",
			},
			scripts: []string{
				`CREATE DATABASE IF NOT EXISTS test_db`,
				`CREATE TABLE IF NOT EXISTS test_db.events (
					event_date Date,
					event_type String,
					user_id UInt64,
					metric_value Float64
				) ENGINE = MergeTree()
				PARTITION BY toYYYYMM(event_date)
				ORDER BY (event_date, event_type, user_id)`,
				`INSERT INTO test_db.events (event_date, event_type, user_id, metric_value) VALUES
					('2024-01-01', 'purchase', 123, 99.99)`,
				`INSERT INTO test_db.events (event_date, event_type, user_id, metric_value) VALUES
					('2024-01-01', 'view', 456, 1.0)`,
				`INSERT INTO test_db.events (event_date, event_type, user_id, metric_value) VALUES
					('2024-01-02', 'purchase', 789, 149.99)`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			if tt.scripts != nil {
				tt.config.InitScripts = tt.scripts
			}

			container, err := NewClickHouse(getClickHouseLogger(), tt.config)
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
			db, err := sql.Open("clickhouse", container.ConnectionString())
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
				query := "SELECT count() FROM test_db.test"
				if tt.name == "with complex schema" {
					query = "SELECT count() FROM test_db.events"
				}

				err := db.QueryRowContext(ctx, query).Scan(&count)
				if err != nil {
					t.Fatalf("Failed to query table: %v", err)
				}

				expectedCount := 1
				if tt.name == "with complex schema" {
					expectedCount = 3
				}

				if count != expectedCount {
					t.Errorf("Expected %d rows in table, got %d", expectedCount, count)
				}
			}

			// Test SQL execution
			if err := container.RunSQL(ctx, "SELECT 1"); err != nil {
				t.Fatalf("Failed to execute SQL: %v", err)
			}

			// Test ClickHouse-specific features
			if tt.name == "with complex schema" {
				var count int
				err := db.QueryRowContext(ctx, `
					SELECT count()
					FROM test_db.events
					WHERE event_type = 'purchase'
					AND metric_value > 100
				`).Scan(&count)

				if err != nil {
					t.Fatalf("Failed to execute ClickHouse-specific query: %v", err)
				}

				if count != 1 {
					t.Errorf("Expected 1 high-value purchase, got %d", count)
				}
			}
		})
	}
}

func TestClickHouseBuilder(t *testing.T) {
	ctx := context.Background()

	t.Run("builder pattern", func(t *testing.T) {
		container, err := NewClickHouseBuilder(getClickHouseLogger()).
			WithVersion("23.8").
			WithDatabase("testdb").
			WithCredentials("user1", "pass123").
			// WithPort(9001).
			WithInitScript(`CREATE DATABASE IF NOT EXISTS testdb`).
			WithInitScript(`
				CREATE TABLE testdb.test (
					id UInt32,
					name String
				) ENGINE = MergeTree()
				ORDER BY id
			`).
			Build(ctx)

		if err != nil {
			t.Fatalf("Failed to build container: %v", err)
		}

		if err := container.Start(ctx); err != nil {
			t.Fatalf("Failed to start container: %v", err)
		}
		defer container.Stop(ctx)

		// Verify configuration
		db, err := sql.Open("clickhouse", container.ConnectionString())
		if err != nil {
			t.Fatalf("Failed to open database connection: %v", err)
		}
		defer db.Close()

		if err := db.PingContext(ctx); err != nil {
			t.Fatalf("Failed to ping database: %v", err)
		}

		// Verify table creation
		var count int
		err = db.QueryRowContext(ctx, "SELECT count() FROM test").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query test table: %v", err)
		}
	})
}
