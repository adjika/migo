package migo_test

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/adjika/migo"
	_ "github.com/lib/pq"
)

var (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "postgres"
	dbname   = "migo_testdata"
)

//go:embed migrations
var migrationDir embed.FS

func TestMigrateCtx(t *testing.T) {
	var count int

	if hostenv, present := os.LookupEnv("POSTGRES_HOST"); present {
		host = hostenv
	}
	if portenv, present := os.LookupEnv("POSTGRES_PORT"); present {
		if i, err := strconv.Atoi(portenv); err == nil {
			port = i
		}
	}
	if userenv, present := os.LookupEnv("POSTGRES_USER"); present {
		user = userenv
	}
	if passwordenv, present := os.LookupEnv("POSTGRES_PASSWORD"); present {
		password = passwordenv
	}
	if dbnameenv, present := os.LookupEnv("POSTGRES_DBNAME"); present {
		dbname = dbnameenv
	}

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		t.Fatalf("Unable to open db: %v", err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		t.Fatalf("Unable to ping db after opening: %v", err)
	}

	if err = migo.Purge(db); err != nil {
		t.Fatalf("Unable to purge migo metadata Purge(db): %v", err)
	}

	if err = migo.MigrateCtx(context.Background(), db, migrationDir); err != nil {
		t.Fatalf("Unable to migrate Migrate(db, migrationDir): %v", err)
	}

	if err = db.QueryRow("SELECT COUNT(*) FROM testdata;").Scan(&count); err == sql.ErrNoRows {
		t.Fatalf("SELECT COUNT(*) on testdata returned no rows: %v", err)
	}

	if count != 2 {
		t.Errorf("testdata table does not have the expected amount of rows, expected: %v, got: %v", 2, count)
	}
}
