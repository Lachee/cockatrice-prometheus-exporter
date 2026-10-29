package cockatrice

import (
	"context"
	"database/sql"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Client struct {
	db *sql.DB

	// Servatrice only records the bytes transferred between statuses, so the running
	// totals are kept here. They start from zero when the exporter starts.
	trafficMu     sync.Mutex
	trafficSeeded bool
	trafficSince  time.Time
	traffic       Traffic

	sessions *tail
	games    *tail
	audit    *tail
}

func NewClient(db *sql.DB) *Client {
	return &Client{
		db:       db,
		sessions: newTail("cockatrice_sessions", "''"),
		games:    newTail("cockatrice_games", "COALESCE(room_name, '')"),
		audit:    newTail("cockatrice_audit", "CONCAT_WS(CHAR(31), action, results)"),
	}
}

type Status struct {
	Uptime     time.Duration
	Users      int
	Moderators int
	Games      int
	Age        time.Duration
}

// LatestUptime gets the most recent uptime from servatrice
func (c *Client) LatestUptime(ctx context.Context) (Status, error) {
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

// LastUptimeRecordedAt gets the time the last uptime was recorded by servatrice.
func (c *Client) LastUptimeRecordedAt(ctx context.Context) (time.Time, error) {
	var t sql.NullTime
	err := c.db.QueryRowContext(ctx, `SELECT MAX(timest) FROM cockatrice_uptime`).Scan(&t)
	return t.Time, err
}

// Traffic is the bytes moved by the server.
type Traffic struct {
	Received uint64
	Sent     uint64
}

// Traffic bytes received and sent since the exporter started.
func (c *Client) Traffic(ctx context.Context) (Traffic, error) {
	// Scrapes can run concurrently, and each status must only be added once
	c.trafficMu.Lock()
	defer c.trafficMu.Unlock()

	if !c.trafficSeeded {
		// Start from the latest status instead of summing the entire history
		since, err := c.LastUptimeRecordedAt(ctx)
		if err != nil {
			return Traffic{}, err
		}
		c.trafficSince, c.trafficSeeded = since, true
	}

	// Each status only holds the bytes since the previous one, so they're summed for a total
	var t Traffic
	var latest sql.NullTime
	err := c.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(rx_bytes), 0), COALESCE(SUM(tx_bytes), 0), MAX(timest)
		FROM cockatrice_uptime
		WHERE timest > ?`,
		c.trafficSince,
	).Scan(&t.Received, &t.Sent, &latest)
	if err != nil {
		return Traffic{}, err
	}
	if latest.Valid {
		c.trafficSince = latest.Time
	}
	c.traffic.Received += t.Received
	c.traffic.Sent += t.Sent
	return c.traffic, nil
}

// SessionsStarted sessions started since the exporter started
func (c *Client) SessionsStarted(ctx context.Context) (uint64, error) {
	totals, err := c.sessions.poll(ctx, c)
	return totals[""], err
}

// GamesStarted games started since the exporter started, by room
func (c *Client) GamesStarted(ctx context.Context) (map[string]uint64, error) {
	return c.games.poll(ctx, c)
}

// AuditSeparator separates the action and result in the AuditEvents keys
const AuditSeparator = "\x1f"

// AuditEvents audited actions since the exporter started, keyed by action and result
// joined with AuditSeparator. Servatrice only records these when audit logging is enabled.
func (c *Client) AuditEvents(ctx context.Context) (map[string]uint64, error) {
	return c.audit.poll(ctx, c)
}

// SessionsActive sessions that have not ended
func (c *Client) SessionsActive(ctx context.Context) (int, error) {
	return c.count(ctx, `SELECT COUNT(*) FROM cockatrice_sessions WHERE end_time IS NULL`)
}

// SessionsActiveByCountry sessions that have not ended, by the country of the account.
// Guests have no account, so they are counted as unknown.
func (c *Client) SessionsActiveByCountry(ctx context.Context) (map[string]int, error) {
	return c.countBy(ctx, `
		SELECT COALESCE(NULLIF(u.country, ''), 'unknown'), COUNT(*)
		FROM cockatrice_sessions s
		LEFT JOIN cockatrice_users u ON u.name = s.user_name
		WHERE s.end_time IS NULL
		GROUP BY 1`)
}

// SessionsActiveByConnection sessions that have not ended, by connection type
func (c *Client) SessionsActiveByConnection(ctx context.Context) (map[string]int, error) {
	return c.countBy(ctx, `SELECT COALESCE(connection_type, 'unknown'), COUNT(*) FROM cockatrice_sessions WHERE end_time IS NULL GROUP BY 1`)
}

// UsersByCountry activated accounts by country
func (c *Client) UsersByCountry(ctx context.Context) (map[string]int, error) {
	return c.countBy(ctx, `SELECT COALESCE(NULLIF(country, ''), 'unknown'), COUNT(*) FROM cockatrice_users WHERE active = 1 GROUP BY 1`)
}

// EmailQueue emails waiting to be sent, by type. A queue that keeps growing means mail is failing.
func (c *Client) EmailQueue(ctx context.Context) (map[string]int, error) {
	return c.countBy(ctx, `
		SELECT 'activation', COUNT(*) FROM cockatrice_activation_emails
		UNION ALL
		SELECT 'password_reset', COUNT(*) FROM cockatrice_forgot_password WHERE emailed = 0`)
}

// ReportsByStatus user reports by status. The reports table only exists on newer servers.
func (c *Client) ReportsByStatus(ctx context.Context) (map[string]int, error) {
	return c.countBy(ctx, `SELECT status, COUNT(*) FROM cockatrice_reports GROUP BY 1`)
}

// UnactivatedUsers accounts that have not been activated
func (c *Client) UnactivatedUsers(ctx context.Context) (int, error) {
	return c.count(ctx, `SELECT COUNT(*) FROM cockatrice_users WHERE active = 0`)
}

// UsersByPrivilege activated accounts by privilege level
func (c *Client) UsersByPrivilege(ctx context.Context) (map[string]int, error) {
	return c.countBy(ctx, `SELECT COALESCE(privlevel, 'NONE'), COUNT(*) FROM cockatrice_users WHERE active = 1 GROUP BY 1`)
}

// StaffByLevel accounts with moderator or admin rights, by admin level
func (c *Client) StaffByLevel(ctx context.Context) (map[string]int, error) {
	return c.countBy(ctx, `SELECT admin, COUNT(*) FROM cockatrice_users WHERE admin > 0 GROUP BY 1`)
}

// ActiveBans bans currently in effect. A ban of zero minutes is permanent.
func (c *Client) ActiveBans(ctx context.Context) (int, error) {
	return c.count(ctx, `SELECT COUNT(*) FROM cockatrice_bans WHERE minutes = 0 OR time_from + INTERVAL minutes MINUTE >= NOW()`)
}

// Bans bans recorded, expired or not
func (c *Client) Bans(ctx context.Context) (int, error) {
	return c.count(ctx, `SELECT COUNT(*) FROM cockatrice_bans`)
}

// Warnings warnings recorded
func (c *Client) Warnings(ctx context.Context) (int, error) {
	return c.count(ctx, `SELECT COUNT(*) FROM cockatrice_warnings`)
}

// Replays game replays recorded
func (c *Client) Replays(ctx context.Context) (int, error) {
	return c.count(ctx, `SELECT COUNT(*) FROM cockatrice_replays`)
}

// ReplaysSize bytes used by the replays table. Summing the replays themselves reads
// every blob, which is far too slow to do on each scrape.
func (c *Client) ReplaysSize(ctx context.Context) (int, error) {
	return c.tableSize(ctx, "cockatrice_replays")
}

// Decks decks stored on the server
func (c *Client) Decks(ctx context.Context) (int, error) {
	return c.count(ctx, `SELECT COUNT(*) FROM cockatrice_decklist_files`)
}

// DecksSize bytes used by the decks table
func (c *Client) DecksSize(ctx context.Context) (int, error) {
	return c.tableSize(ctx, "cockatrice_decklist_files")
}

// DeckFolders deck folders stored on the server
func (c *Client) DeckFolders(ctx context.Context) (int, error) {
	return c.count(ctx, `SELECT COUNT(*) FROM cockatrice_decklist_folders`)
}

// BuddyEntries entries across all buddy lists
func (c *Client) BuddyEntries(ctx context.Context) (int, error) {
	return c.count(ctx, `SELECT COUNT(*) FROM cockatrice_buddylist`)
}

// IgnoreEntries entries across all ignore lists
func (c *Client) IgnoreEntries(ctx context.Context) (int, error) {
	return c.count(ctx, `SELECT COUNT(*) FROM cockatrice_ignorelist`)
}

// Rooms chat rooms on the server
func (c *Client) Rooms(ctx context.Context) (int, error) {
	return c.count(ctx, `SELECT COUNT(*) FROM cockatrice_rooms`)
}

// Ping checks the database is reachable
func (c *Client) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// PoolStats connection pool statistics
func (c *Client) PoolStats() sql.DBStats {
	return c.db.Stats()
}

// count runs a query that returns a single number. The query must be a trusted constant.
func (c *Client) count(ctx context.Context, query string) (int, error) {
	var n int
	err := c.db.QueryRowContext(ctx, query).Scan(&n)
	return n, err
}

// tableSize bytes used by a table's data, from the table statistics.
// MySQL caches these, so it can lag behind (information_schema_stats_expiry).
func (c *Client) tableSize(ctx context.Context, table string) (int, error) {
	var n int
	err := c.db.QueryRowContext(ctx, `
		SELECT COALESCE(DATA_LENGTH, 0)
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`,
		table,
	).Scan(&n)
	return n, err
}

// countBy runs a query that returns a label and a number per row. The query must be a trusted constant.
func (c *Client) countBy(ctx context.Context, query string) (map[string]int, error) {
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := map[string]int{}
	for rows.Next() {
		var l string
		var n int
		if err := rows.Scan(&l, &n); err != nil {
			return nil, err
		}
		counts[l] = n
	}
	return counts, rows.Err()
}

// maxID highest id in a table, or 0 if it's empty. The table must be a trusted constant.
func (c *Client) maxID(ctx context.Context, table string) (int64, error) {
	var id sql.NullInt64
	err := c.db.QueryRowContext(ctx, `SELECT MAX(id) FROM `+table).Scan(&id)
	return id.Int64, err
}

// newSince counts the rows with an id above after, grouped by the label expression.
// Also returns the highest id seen, to pass as after on the next call.
// The table and label must be trusted constants.
func (c *Client) newSince(ctx context.Context, table, label string, after int64) (map[string]uint64, int64, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT `+label+`, COUNT(*), MAX(id) FROM `+table+` WHERE id > ? GROUP BY 1`, after)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()

	counts := map[string]uint64{}
	latest := after
	for rows.Next() {
		var l string
		var n uint64
		var id int64
		if err := rows.Scan(&l, &n, &id); err != nil {
			return nil, after, err
		}
		counts[l] = n
		latest = max(latest, id)
	}
	return counts, latest, rows.Err()
}

// tail counts new rows in a table, using the auto-incrementing id as a cursor.
// Counts start from zero when the exporter starts.
type tail struct {
	table, label string

	mu     sync.Mutex
	seeded bool
	after  int64
	totals map[string]uint64
}

func newTail(table, label string) *tail {
	return &tail{table: table, label: label, totals: map[string]uint64{}}
}

// poll adds the rows created since the last poll, and returns a copy of the totals.
func (t *tail) poll(ctx context.Context, c *Client) (map[string]uint64, error) {
	// Scrapes can run concurrently, and each row must only be counted once
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.seeded {
		// Start from the latest row instead of counting the entire history
		id, err := c.maxID(ctx, t.table)
		if err != nil {
			return nil, err
		}
		t.after, t.seeded = id, true
	}

	counts, latest, err := c.newSince(ctx, t.table, t.label, t.after)
	if err != nil {
		return nil, err
	}
	t.after = latest
	for l, n := range counts {
		t.totals[l] += n
	}

	out := make(map[string]uint64, len(t.totals))
	for l, n := range t.totals {
		out[l] = n
	}
	return out, nil
}

// RegisteredUsers number of accounts registered
func (c *Client) RegisteredUsers(ctx context.Context) (int, error) {
	var n int
	err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cockatrice_users WHERE active = 1`).Scan(&n)
	return n, err
}
