# dbcontainers

A robust, flexible Go library for running database containers in tests. Supports PostgreSQL, MySQL, MariaDB, and ClickHouse.

## Features

- Simple, fluent API for container configuration
- Supports multiple database types:
    - PostgreSQL
    - MySQL
    - MariaDB
    - ClickHouse
- Robust container orchestration with health checks and retry logic
- VM-aware networking for Docker Machine environments
- Support for initialization scripts (SQL files and embedded FS)
- Detailed logging and error reporting

## Installation

```bash
go get -u github.com/panta/dbcontainers
```

## Quick Start

```go
package main

import (
    "context"
    "database/sql"
    "testing"
    
    "github.com/panta/dbcontainers"
)

func TestWithPostgres(t *testing.T) {
    ctx := context.Background()
    
    // Create and start a Postgres container
    container, err := dbcontainers.NewPostgresBuiler(nil).
        WithVersion("15-bullseye").
        WithDatabase("testdb").
        WithCredentials("testuser", "testpass").
        WithInitScript(`
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT
            );
        `).
        Build(ctx)
    
    if err != nil {
        t.Fatal(err)
    }
    defer container.Stop(ctx)
    
    // Use the container
    db, err := sql.Open("postgres", container.ConnectionString())
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()
    
    // Run your tests...
}
```

## Database Adapters

### PostgreSQL

```go
container, err := dbcontainers.NewPostgresBuiler(nil).
    WithVersion("15-bullseye").
    WithDatabase("mydb").
    WithCredentials("user", "pass").
    Build(ctx)
```

### MySQL

```go
container, err := dbcontainers.NewMySQLBuilder(nil).
    WithVersion("8").
    WithDatabase("mydb").
    WithCredentials("user", "pass").
    Build(ctx)
```

### MariaDB

```go
container, err := dbcontainers.NewMariaDBBuiler(nil).
    WithVersion("10.11").
    WithDatabase("mydb").
    WithCredentials("user", "pass").
    Build(ctx)
```

### ClickHouse

```go
container, err := dbcontainers.NewClickHouseBuilder(nil).
    WithVersion("23.8").
    WithDatabase("mydb").
    WithCredentials("user", "pass").
    Build(ctx)
```

## Advanced Usage

### Initialization Scripts

```go
// From string
builder.WithInitScript("CREATE TABLE users (id SERIAL PRIMARY KEY);")

// From file
builder.WithInitScriptFile("testdata/schema.sql")

// From multiple files
builder.WithInitScriptFiles([]string{
    "testdata/schema.sql",
    "testdata/seed.sql",
})

// From embedded FS
//go:embed testdata/init/*.sql
var initFS embed.FS

builder.WithInitScriptFS(initFS, "testdata/init/*.sql")
```

### VM-Aware Networking

The library automatically handles Docker networking in VM environments.

The docker host is determined automatically, but in case you need to override or force it:
```go
builder.WithDockerHost("192.168.99.100") // For Docker Machine
```

### Custom Retry Policy

```go
builder.WithRetryPolicy(
    5,                  // attempts
    time.Second,        // delay
    time.Minute,        // timeout
)
```

### Debug Logging

You can optionally pass a [slog](https://go.dev/blog/slog) handler to the builders:

```go
logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))

container, err := dbcontainers.NewPostgresBuiler(logger).
    WithDebug(true).
    Build(ctx)
```

### Container Interface

All database containers implement this interface:

```go
type Container interface {
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    ConnectionString() string
    Logs(ctx context.Context) ([]byte, error)
    RunSQL(ctx context.Context, script string) error
}
```

## Integration

dbcontainers works seamlessly with other testing libraries. See the [INTEGRATION.md](INTEGRATION.md) document for
more information.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the Apache License Version 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by [testcontainers](https://www.testcontainers.org/)
- Uses [Docker Engine API](https://docs.docker.com/engine/api/) for container management
