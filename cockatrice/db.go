package cockatrice

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Client struct {
	db *sql.DB
}

// NewClient acts as a constructor, injecting the active database connection.
func NewClient(db *sql.DB) *Client {
	return &Client{db: db}
}

// Status is a status update that servatrice periodically writes to the uptime table.
type Status struct {
	Uptime     time.Duration
	Users      int
	Moderators int
	Games      int
	Age        time.Duration
}

// LatestStatus most recent status
func (c *Client) LatestStatus(ctx context.Context) (Status, error) {
	var s Status
	var uptime, age int64
	err := c.db.QueryRowContext(ctx, `
		SELECT uptime, users_count, mods_count, games_count, TIMESTAMPDIFF(SECOND, timest, NOW())
		FROM cockatrice_uptime
		ORDER BY timest DESC
		LIMIT 1`,
	).Scan(&uptime, &s.Users, &s.Moderators, &s.Games, &age)
	s.Uptime = time.Duration(uptime) * time.Second
	s.Age = time.Duration(age) * time.Second
	return s, err
}

// RegisteredUsers number of accounts registered
func (c *Client) RegisteredUsers(ctx context.Context) (int, error) {
	var n int
	err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cockatrice_users WHERE active = 1`).Scan(&n)
	return n, err
}
