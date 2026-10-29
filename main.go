package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/lachee/cockatrice-prometheus-exporter/v2/cockatrice"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func registerAndServePrometheus(client *cockatrice.Client) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(cockatrice.NewCollector(client))
	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	log.Fatal(http.ListenAndServe(":2112", nil))
}

func main() {
	log.SetOutput(os.Stdout)

	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, relying on system environment variables")
	}

	// Load the environment variables
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "3306"
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&loc=UTC", user, password, host, port, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Pool init failed: %v", err)
	}
	defer db.Close()

	// Configure pool parameters
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		log.Fatalf("Database unreachable: %v", err)
	}

	client := cockatrice.NewClient(db)
	registerAndServePrometheus(client)
}
