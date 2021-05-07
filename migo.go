package migo

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
)

const (
	idLength          = 100
	metadataTableName = "migo_metadata"
)

// MigrateCtx takes a fs.FS containing sql files and runs all .sql files
// not ending with _rollback.sql on the provided db.
// Migration files should begin with an integer, optionally followed by
// an underscore and a description, and end with .sql.
// Migration files must not end with _rollback.sql
func MigrateCtx(ctx context.Context, db *sql.DB, fsys fs.FS) error {
	err := createMetadataTable(db)
	if err != nil {
		return fmt.Errorf("could not create or check for metadata table: %w", err)
	}

	migrationFiles, err := fs.ReadDir(fsys, "migrations")
	if err != nil {
		return err
	}

	for _, migrationFile := range migrationFiles {
		if migrationFile.Type().IsRegular() {
			id, err := getMigrationId(migrationFile.Name())
			if err != nil {
				continue
			}
			// do stuff.. for now just print the id
			fmt.Println(id)
		}
	}
	return nil
}

// Migrate is equivalent to calling MigrateCtx(context.TODO(), db, fsys)
func Migrate(db *sql.DB, fsys fs.FS) error {
	return MigrateCtx(context.TODO(), db, fsys)
}

// Purge deletes all migo specific information from the db, leaving previously run migrations intact
func Purge(db *sql.DB) error {
	return dropMetadataTable(db)
}

func createMetadataTable(db *sql.DB) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS " + metadataTableName + " (id int PRIMARY KEY, name VARCHAR(" + fmt.Sprint(idLength) + ") UNIQUE, migrated_at VARCHAR(30))")
	return err
}

func dropMetadataTable(db *sql.DB) error {
	_, err := db.Exec("DROP TABLE IF EXISTS " + metadataTableName)
	return err
}

func getMigrationId(filename string) (string, error) {
	if !strings.HasSuffix(filename, ".sql") {
		return "-1", fmt.Errorf("not a sql file, %v", filename)
	}
	if strings.HasSuffix(filename, "_rollback.sql") {
		return "-1", fmt.Errorf("rollback file, %v", filename)
	}
	filename = strings.TrimSuffix(filename, ".sql")

	if _, err := strconv.Atoi(strings.Split(filename, "_")[0]); err != nil {
		return "-1", fmt.Errorf("filename is not prefixed by an integer, %v", filename)
	}

	if len(filename) > 100 {
		filename = filename[0:99]
	}

	return filename, nil
}

func getRollbackId(filename string) (string, error) {
	if !strings.HasSuffix(filename, ".sql") {
		return "-1", fmt.Errorf("not a sql file, %v", filename)
	}
	if !strings.HasSuffix(filename, "_rollback.sql") {
		return "-1", fmt.Errorf("not a rollback file, %v", filename)
	}
	filename = strings.TrimSuffix(filename, "_rollback.sql")

	if _, err := strconv.Atoi(strings.Split(filename, "_")[0]); err != nil {
		return "-1", fmt.Errorf("filename is not prefixed by an integer, %v", filename)
	}

	if len(filename) > 100 {
		filename = filename[0:99]
	}

	return filename, nil
}
