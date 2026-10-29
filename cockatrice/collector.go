package cockatrice

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// staleAfter is how old the latest status can be before the server is considered down.
// Servatrice writes a status every 15 seconds by default (server/statusupdate).
const staleAfter = 60 * time.Second

type Collector struct {
	client *Client

	up              *prometheus.Desc
	statusAge       *prometheus.Desc
	uptime          *prometheus.Desc
	restarts        *prometheus.Desc
	bytesReceived   *prometheus.Desc
	bytesSent       *prometheus.Desc
	usersOnline     *prometheus.Desc
	modsOnline      *prometheus.Desc
	gamesActive     *prometheus.Desc
	usersRegistered *prometheus.Desc

	sessionsActive  *prometheus.Desc
	sessionsCountry *prometheus.Desc
	sessionsConn    *prometheus.Desc
	usersCountry    *prometheus.Desc
	emailQueue      *prometheus.Desc
	reports         *prometheus.Desc
	auditEvents     *prometheus.Desc
	sessionsStarted *prometheus.Desc
	gamesStarted    *prometheus.Desc
	usersUnactive   *prometheus.Desc
	usersPrivilege  *prometheus.Desc
	usersStaff      *prometheus.Desc
	bansActive      *prometheus.Desc
	bans            *prometheus.Desc
	warnings        *prometheus.Desc
	replays         *prometheus.Desc
	replaysSize     *prometheus.Desc
	decks           *prometheus.Desc
	decksSize       *prometheus.Desc
	deckFolders     *prometheus.Desc
	buddies         *prometheus.Desc
	ignores         *prometheus.Desc
	rooms           *prometheus.Desc

	dbUp           *prometheus.Desc
	scrapeDuration *prometheus.Desc
	scrapeErrors   *prometheus.Desc
	dbOpen         *prometheus.Desc
	dbInUse        *prometheus.Desc

	// Restarts are noticed when the uptime goes backwards between scrapes
	restartMu  sync.Mutex
	lastUptime time.Duration
	restartsN  uint64

	errMu  sync.Mutex
	errors map[string]uint64
}

func NewCollector(c *Client) *Collector {
	desc := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc("cockatrice_"+name, help, labels, nil)
	}
	return &Collector{
		client:          c,
		up:              desc("up", "Is the server currently running"),
		statusAge:       desc("status_age_seconds", "Seconds since the server last wrote a status update"),
		uptime:          desc("uptime_seconds", "Seconds since the server started"),
		restarts:        desc("server_restarts_total", "Server restarts seen by the exporter since it started"),
		bytesReceived:   desc("received_bytes_total", "Bytes received by the server since the exporter started"),
		bytesSent:       desc("sent_bytes_total", "Bytes sent by the server since the exporter started"),
		usersOnline:     desc("users_online", "Users currently connected"),
		modsOnline:      desc("moderators_online", "Moderators currently connected"),
		gamesActive:     desc("games_active", "Games currently in progress"),
		usersRegistered: desc("users_registered", "Activated user accounts"),

		sessionsActive:  desc("sessions_active", "Sessions that have not ended"),
		sessionsCountry: desc("sessions_active_by_country", "Sessions that have not ended, by the country of the account", "country"),
		sessionsConn:    desc("sessions_active_by_connection", "Sessions that have not ended, by connection type", "type"),
		usersCountry:    desc("users_by_country", "Activated accounts by country", "country"),
		emailQueue:      desc("email_queue", "Emails waiting to be sent, by type", "type"),
		reports:         desc("reports", "User reports by status", "status"),
		auditEvents:     desc("audit_events_total", "Audited actions since the exporter started, by action and result", "action", "result"),
		sessionsStarted: desc("sessions_started_total", "Sessions started since the exporter started"),
		gamesStarted:    desc("games_started_total", "Games started since the exporter started", "room"),
		usersUnactive:   desc("users_unactivated", "Accounts that have not been activated"),
		usersPrivilege:  desc("users_by_privilege", "Activated accounts by privilege level", "level"),
		usersStaff:      desc("users_staff", "Accounts with moderator or admin rights, by admin level", "level"),
		bansActive:      desc("bans_active", "Bans currently in effect"),
		bans:            desc("bans_total", "Bans ever issued and still recorded"),
		warnings:        desc("warnings_total", "Warnings recorded"),
		replays:         desc("replays_total", "Game replays recorded"),
		replaysSize:     desc("replays_size_bytes", "Total size of the game replays recorded"),
		decks:           desc("decks_stored", "Decks stored on the server"),
		decksSize:       desc("decks_size_bytes", "Total size of the decks stored on the server"),
		deckFolders:     desc("deck_folders_stored", "Deck folders stored on the server"),
		buddies:         desc("buddy_entries", "Entries across all buddy lists"),
		ignores:         desc("ignore_entries", "Entries across all ignore lists"),
		rooms:           desc("rooms", "Chat rooms on the server"),

		dbUp:           desc("exporter_db_up", "Is the database reachable"),
		scrapeDuration: desc("exporter_scrape_duration_seconds", "Time taken to collect the metrics"),
		scrapeErrors:   desc("exporter_scrape_errors_total", "Failed queries, by query", "query"),
		dbOpen:         desc("exporter_db_open_connections", "Open database connections"),
		dbInUse:        desc("exporter_db_in_use_connections", "Database connections currently in use"),

		errors: map[string]uint64{},
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.up, c.statusAge, c.uptime, c.restarts, c.bytesReceived, c.bytesSent,
		c.usersOnline, c.modsOnline, c.gamesActive, c.usersRegistered,
		c.sessionsActive, c.sessionsStarted, c.gamesStarted, c.usersUnactive,
		c.sessionsCountry, c.sessionsConn, c.usersCountry, c.emailQueue, c.reports, c.auditEvents,
		c.usersPrivilege, c.usersStaff, c.bansActive, c.bans, c.warnings, c.replays,
		c.replaysSize, c.decks, c.decksSize, c.deckFolders, c.buddies, c.ignores, c.rooms,
		c.dbUp, c.scrapeDuration, c.scrapeErrors, c.dbOpen, c.dbInUse,
	} {
		ch <- d
	}
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c.collectStatus(ctx, ch)
	c.collectTraffic(ctx, ch)

	registered, err := c.client.RegisteredUsers(ctx)
	if err != nil {
		c.countError("registered_users")
		ch <- prometheus.NewInvalidMetric(c.usersRegistered, err)
	} else {
		ch <- prometheus.MustNewConstMetric(c.usersRegistered, prometheus.GaugeValue, float64(registered))
	}

	c.collectCounts(ctx, ch)
	c.collectTails(ctx, ch)
	c.collectExporter(ctx, ch, start)
}

func (c *Collector) collectStatus(ctx context.Context, ch chan<- prometheus.Metric) {
	status, err := c.client.LatestUptime(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		// Servatrice has never reported in
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}
	if err != nil {
		// Fails the scrape, so Prometheus marks the target as down
		c.countError("status")
		ch <- prometheus.NewInvalidMetric(c.up, err)
		return
	}

	ch <- prometheus.MustNewConstMetric(c.statusAge, prometheus.GaugeValue, status.Age.Seconds())
	if status.Age > staleAfter {
		// The remaining values are from before the server went down, so don't report them
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}

	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(c.uptime, prometheus.GaugeValue, status.Uptime.Seconds())
	ch <- prometheus.MustNewConstMetric(c.usersOnline, prometheus.GaugeValue, float64(status.Users))
	ch <- prometheus.MustNewConstMetric(c.modsOnline, prometheus.GaugeValue, float64(status.Moderators))
	ch <- prometheus.MustNewConstMetric(c.gamesActive, prometheus.GaugeValue, float64(status.Games))

	c.restartMu.Lock()
	defer c.restartMu.Unlock()
	if status.Uptime < c.lastUptime {
		c.restartsN++
	}
	c.lastUptime = status.Uptime
	ch <- prometheus.MustNewConstMetric(c.restarts, prometheus.CounterValue, float64(c.restartsN))
}

func (c *Collector) collectTraffic(ctx context.Context, ch chan<- prometheus.Metric) {
	t, err := c.client.Traffic(ctx)
	if err != nil {
		c.countError("traffic")
		ch <- prometheus.NewInvalidMetric(c.bytesReceived, err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.bytesReceived, prometheus.CounterValue, float64(t.Received))
	ch <- prometheus.MustNewConstMetric(c.bytesSent, prometheus.CounterValue, float64(t.Sent))
}

// collectCounts failures are counted and skipped, rather than failing the whole scrape.
func (c *Collector) collectCounts(ctx context.Context, ch chan<- prometheus.Metric) {
	for _, m := range []struct {
		name  string
		desc  *prometheus.Desc
		typ   prometheus.ValueType
		query func(context.Context) (int, error)
	}{
		{"sessions_active", c.sessionsActive, prometheus.GaugeValue, c.client.SessionsActive},
		{"unactivated", c.usersUnactive, prometheus.GaugeValue, c.client.UnactivatedUsers},
		{"bans_active", c.bansActive, prometheus.GaugeValue, c.client.ActiveBans},
		{"bans", c.bans, prometheus.GaugeValue, c.client.Bans},
		{"warnings", c.warnings, prometheus.CounterValue, c.client.Warnings},
		{"replays", c.replays, prometheus.CounterValue, c.client.Replays},
		{"replays_size", c.replaysSize, prometheus.GaugeValue, c.client.ReplaysSize},
		{"decks", c.decks, prometheus.GaugeValue, c.client.Decks},
		{"decks_size", c.decksSize, prometheus.GaugeValue, c.client.DecksSize},
		{"deck_folders", c.deckFolders, prometheus.GaugeValue, c.client.DeckFolders},
		{"buddies", c.buddies, prometheus.GaugeValue, c.client.BuddyEntries},
		{"ignores", c.ignores, prometheus.GaugeValue, c.client.IgnoreEntries},
		{"rooms", c.rooms, prometheus.GaugeValue, c.client.Rooms},
	} {
		n, err := m.query(ctx)
		if err != nil {
			c.countError(m.name)
			continue
		}
		ch <- prometheus.MustNewConstMetric(m.desc, m.typ, float64(n))
	}

	for _, m := range []struct {
		name  string
		desc  *prometheus.Desc
		query func(context.Context) (map[string]int, error)
	}{
		{"privileges", c.usersPrivilege, c.client.UsersByPrivilege},
		{"staff", c.usersStaff, c.client.StaffByLevel},
		{"sessions_by_country", c.sessionsCountry, c.client.SessionsActiveByCountry},
		{"sessions_by_connection", c.sessionsConn, c.client.SessionsActiveByConnection},
		{"users_by_country", c.usersCountry, c.client.UsersByCountry},
		{"email_queue", c.emailQueue, c.client.EmailQueue},
		{"reports", c.reports, c.client.ReportsByStatus},
	} {
		counts, err := m.query(ctx)
		if err != nil {
			c.countError(m.name)
			continue
		}
		for label, n := range counts {
			ch <- prometheus.MustNewConstMetric(m.desc, prometheus.GaugeValue, float64(n), label)
		}
	}
}

func (c *Collector) collectTails(ctx context.Context, ch chan<- prometheus.Metric) {
	if sessions, err := c.client.SessionsStarted(ctx); err != nil {
		c.countError("sessions_started")
	} else {
		ch <- prometheus.MustNewConstMetric(c.sessionsStarted, prometheus.CounterValue, float64(sessions))
	}

	if games, err := c.client.GamesStarted(ctx); err != nil {
		c.countError("games_started")
	} else {
		for room, n := range games {
			ch <- prometheus.MustNewConstMetric(c.gamesStarted, prometheus.CounterValue, float64(n), room)
		}
	}

	if events, err := c.client.AuditEvents(ctx); err != nil {
		c.countError("audit_events")
	} else {
		for key, n := range events {
			action, result, _ := strings.Cut(key, AuditSeparator)
			ch <- prometheus.MustNewConstMetric(c.auditEvents, prometheus.CounterValue, float64(n), action, result)
		}
	}
}

func (c *Collector) collectExporter(ctx context.Context, ch chan<- prometheus.Metric, start time.Time) {
	dbUp := 1.0
	if err := c.client.Ping(ctx); err != nil {
		dbUp = 0
	}
	ch <- prometheus.MustNewConstMetric(c.dbUp, prometheus.GaugeValue, dbUp)

	stats := c.client.PoolStats()
	ch <- prometheus.MustNewConstMetric(c.dbOpen, prometheus.GaugeValue, float64(stats.OpenConnections))
	ch <- prometheus.MustNewConstMetric(c.dbInUse, prometheus.GaugeValue, float64(stats.InUse))

	c.errMu.Lock()
	queries := make([]string, 0, len(c.errors))
	for q := range c.errors {
		queries = append(queries, q)
	}
	sort.Strings(queries)
	for _, q := range queries {
		ch <- prometheus.MustNewConstMetric(c.scrapeErrors, prometheus.CounterValue, float64(c.errors[q]), q)
	}
	c.errMu.Unlock()

	ch <- prometheus.MustNewConstMetric(c.scrapeDuration, prometheus.GaugeValue, time.Since(start).Seconds())
}

func (c *Collector) countError(query string) {
	c.errMu.Lock()
	c.errors[query]++
	c.errMu.Unlock()
}
