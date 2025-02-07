# Integration with Testing Libraries

While dbcontainers focuses on container management for database testing, it works seamlessly with other testing
libraries in the Go ecosystem. This section demonstrates how to integrate dbcontainers with popular testing tools.

## Integration with go-testfixtures

[go-testfixtures](https://github.com/go-testfixtures/testfixtures) is a mature library for loading database test
fixtures. It's particularly useful when you need to populate your test database with complex data sets.

### Installation

```bash
go get -u github.com/go-testfixtures/testfixtures/v3
```

### Example Usage

First, create your fixtures in YAML format:

```yaml
# testdata/fixtures/users.yml
- id: 1
  name: John Doe
  email: john@example.com
  
- id: 2
  name: Jane Smith
  email: jane@example.com
```

Then use it in your tests:

```go
package integration

import (
    "context"
    "database/sql"
    "testing"
    
    "github.com/go-testfixtures/testfixtures/v3"
    "github.com/panta/dbcontainers"
)

func TestWithFixtures(t *testing.T) {
    ctx := context.Background()
    
    // Start PostgreSQL container
    container, err := dbcontainers.NewPostgresBuilder(nil).
        WithDatabase("testdb").
        WithInitScript(`
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            );
        `).
        Build(ctx)
    if err != nil {
        t.Fatal(err)
    }
    defer container.Stop(ctx)

    // Open database connection
    db, err := sql.Open("postgres", container.ConnectionString())
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()

    // Initialize fixtures
    fixtures, err := testfixtures.New(
        testfixtures.Database(db),
        testfixtures.Directory("testdata/fixtures"),
        testfixtures.DangerousSkipTestDatabaseCheck(),
    )
    if err != nil {
        t.Fatal(err)
    }

    // Load fixtures
    if err := fixtures.Load(); err != nil {
        t.Fatal(err)
    }

    // Run your tests with the populated database
    var count int
    err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
    if err != nil {
        t.Fatal(err)
    }
    if count != 2 {
        t.Errorf("expected 2 users, got %d", count)
    }
}
```

## Integration with gotest.tools/golden

[gotest.tools/golden](https://pkg.go.dev/gotest.tools/v3/golden) is useful for comparing test outputs with expected results. This is particularly valuable when testing complex database queries.

### Installation

```bash
go get -u gotest.tools/v3
```

### Example Usage

```go
package integration

import (
    "context"
    "database/sql"
    "encoding/json"
    "testing"
    
    "gotest.tools/v3/golden"
    "github.com/panta/dbcontainers"
)

type UserStats struct {
    TotalUsers     int     `json:"total_users"`
    AverageAge     float64 `json:"average_age"`
    CountryStats   []struct {
        Country string `json:"country"`
        Count   int    `json:"count"`
    } `json:"country_stats"`
}

func TestComplexQueryWithGolden(t *testing.T) {
    ctx := context.Background()
    
    // Start PostgreSQL container
    container, err := dbcontainers.NewPostgresBuilder(nil).
        WithDatabase("testdb").
        WithInitScript(`
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                age INT NOT NULL,
                country TEXT NOT NULL
            );
            
            INSERT INTO users (name, age, country) VALUES
                ('John Doe', 30, 'USA'),
                ('Jane Smith', 25, 'UK'),
                ('Alice Johnson', 35, 'USA');
        `).
        Build(ctx)
    if err != nil {
        t.Fatal(err)
    }
    defer container.Stop(ctx)

    // Open database connection
    db, err := sql.Open("postgres", container.ConnectionString())
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()

    // Run complex query
    rows, err := db.Query(`
        WITH country_counts AS (
            SELECT country, COUNT(*) as count
            FROM users
            GROUP BY country
            ORDER BY count DESC, country
        )
        SELECT
            (SELECT COUNT(*) FROM users) as total_users,
            (SELECT AVG(age) FROM users) as average_age,
            json_agg(json_build_object(
                'country', country,
                'count', count
            )) as country_stats
        FROM country_counts;
    `)
    if err != nil {
        t.Fatal(err)
    }
    defer rows.Close()

    // Parse results
    var stats UserStats
    if rows.Next() {
        var countryStatsJSON []byte
        err := rows.Scan(
            &stats.TotalUsers,
            &stats.AverageAge,
            &countryStatsJSON,
        )
        if err != nil {
            t.Fatal(err)
        }
        err = json.Unmarshal(countryStatsJSON, &stats.CountryStats)
        if err != nil {
            t.Fatal(err)
        }
    }

    // Convert to JSON for comparison
    got, err := json.MarshalIndent(stats, "", "  ")
    if err != nil {
        t.Fatal(err)
    }

    // Compare with golden file
    golden.Assert(t, string(got), "testdata/stats.golden.json")
}
```

Example golden file (`testdata/stats.golden.json`):
```json
{
  "total_users": 3,
  "average_age": 30,
  "country_stats": [
    {
      "country": "USA",
      "count": 2
    },
    {
      "country": "UK",
      "count": 1
    }
  ]
}
```

## Integration with go-sqlmock

[go-sqlmock](https://github.com/DATA-DOG/go-sqlmock) is useful when you want to mix container-based testing with mock-based testing. This is particularly valuable when you want to test edge cases or error conditions without setting up complex database states.

### Installation

```bash
go get -u github.com/DATA-DOG/go-sqlmock
```

### Example Usage

Here's an example showing how to test the same code with both dbcontainers and go-sqlmock:

```go
package db

import (
    "context"
    "database/sql"
    "testing"
    
    "github.com/DATA-DOG/go-sqlmock"
    "github.com/panta/dbcontainers"
)

// UserRepository handles database operations for users
type UserRepository struct {
    db *sql.DB
}

func NewUserRepository(db *sql.DB) *UserRepository {
    return &UserRepository{db: db}
}

func (r *UserRepository) GetUserByID(ctx context.Context, id int) (string, error) {
    var name string
    err := r.db.QueryRowContext(ctx, "SELECT name FROM users WHERE id = $1", id).Scan(&name)
    return name, err
}

// Test with real database container
func TestUserRepository_GetUserByID_WithContainer(t *testing.T) {
    ctx := context.Background()
    
    // Start PostgreSQL container
    container, err := dbcontainers.NewPostgresBuilder(nil).
        WithDatabase("testdb").
        WithInitScript(`
            CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
            INSERT INTO users (id, name) VALUES (1, 'John Doe');
        `).
        Build(ctx)
    if err != nil {
        t.Fatal(err)
    }
    defer container.Stop(ctx)

    // Create repository with real database
    db, err := sql.Open("postgres", container.ConnectionString())
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()

    repo := NewUserRepository(db)

    // Test with real database
    name, err := repo.GetUserByID(ctx, 1)
    if err != nil {
        t.Fatal(err)
    }
    if name != "John Doe" {
        t.Errorf("expected 'John Doe', got %q", name)
    }
}

// Test with mock database
func TestUserRepository_GetUserByID_WithMock(t *testing.T) {
    // Create mock database
    db, mock, err := sqlmock.New()
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()

    // Set up expectations
    mock.ExpectQuery("SELECT name FROM users WHERE id = \\$1").
        WithArgs(1).
        WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("John Doe"))

    // Create repository with mock database
    repo := NewUserRepository(db)

    // Test with mock
    name, err := repo.GetUserByID(context.Background(), 1)
    if err != nil {
        t.Fatal(err)
    }
    if name != "John Doe" {
        t.Errorf("expected 'John Doe', got %q", name)
    }

    // Verify all expectations were met
    if err := mock.ExpectationsWereMet(); err != nil {
        t.Errorf("unmet mock expectations: %v", err)
    }
}
```

With this approach it's possible to:

- use dbcontainers for integration tests with real database behavior
- use go-sqlmock for unit tests and testing error conditions
- share the same repository code between both test types

## Best Practices

1. **Test Organization**:
    - Keep container-based tests in separate files (e.g., `*_integration_test.go`)
    - Use build tags to separate integration tests: `//go:build integration`
    - Run integration tests explicitly: `go test -tags=integration`

2. **Resource Management**:
    - Always use `defer container.Stop(ctx)` to clean up containers
    - Consider using `t.Parallel()` with dbcontainers for parallel test execution
    - Use appropriate timeouts in your context

3. **Data Management**:
    - Use go-testfixtures for complex test data scenarios
    - Use golden files for verifying complex query results
    - Use go-sqlmock for testing error conditions and edge cases

4. **Performance**:
    - Reuse containers across related tests when possible
    - Use transactions for test isolation when appropriate
    - Consider using template databases for faster test setup
