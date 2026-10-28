package cockatrice

import (
	"context"
	"database/sql"
	"errors"
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
	usersOnline     *prometheus.Desc
	modsOnline      *prometheus.Desc
	gamesActive     *prometheus.Desc
	usersRegistered *prometheus.Desc
}

func NewCollector(c *Client) *Collector {
	return &Collector{
		client:          c,
		up:              prometheus.NewDesc("cockatrice_up", "Is the server currently running", nil, nil),
		statusAge:       prometheus.NewDesc("cockatrice_status_age_seconds", "Seconds since the server last wrote a status update", nil, nil),
		uptime:          prometheus.NewDesc("cockatrice_uptime_seconds", "Seconds since the server started", nil, nil),
		usersOnline:     prometheus.NewDesc("cockatrice_users_online", "Users currently connected", nil, nil),
		modsOnline:      prometheus.NewDesc("cockatrice_moderators_online", "Moderators currently connected", nil, nil),
		gamesActive:     prometheus.NewDesc("cockatrice_games_active", "Games currently in progress", nil, nil),
		usersRegistered: prometheus.NewDesc("cockatrice_users_registered", "Activated user accounts", nil, nil),
	}
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.statusAge
	ch <- c.uptime
	ch <- c.usersOnline
	ch <- c.modsOnline
	ch <- c.gamesActive
	ch <- c.usersRegistered
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c.collectStatus(ctx, ch)

	registered, err := c.client.RegisteredUsers(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.usersRegistered, err)
	} else {
		ch <- prometheus.MustNewConstMetric(c.usersRegistered, prometheus.GaugeValue, float64(registered))
	}
}

func (c *Collector) collectStatus(ctx context.Context, ch chan<- prometheus.Metric) {
	status, err := c.client.LatestStatus(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		// Servatrice has never reported in
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}
	if err != nil {
		// Fails the scrape, so Prometheus marks the target as down
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
}
