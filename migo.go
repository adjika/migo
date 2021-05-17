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
		if migrationFile.Type().IsRegular() && isMigrationFile(migrationFile.Name()) {
			id, err := getId(migrationFile.Name())
			if err != nil {
				continue
			}

			needsMigration, err := needsMigration(ctx, db, id)
			if err != nil || !needsMigration {
				continue
			}
			// do stuff.. for now just print the filename
			fmt.Printf("Migrating %v\n", migrationFile.Name())
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

func getId(filename string) (int, error) {
	if !strings.HasSuffix(filename, ".sql") {
		return -1, fmt.Errorf("not a sql file, %v", filename)
	}

	filename = strings.TrimSuffix(filename, ".sql")
	id, err := strconv.Atoi(strings.Split(filename, "_")[0])
	if err != nil {
		return -1, fmt.Errorf("filename is not prefixed by an integer, %v", filename)
	}

	return id, nil
}

func isMigrationFile(filename string) bool {

	if !strings.HasSuffix(filename, ".sql") {
		return false
	}
	if strings.HasSuffix(filename, "_rollback.sql") {
		return false
	}

	if _, err := getId(filename); err != nil {
		return false
	}

	return true
}

func isRollbackFile(filename string) bool {
	if !strings.HasSuffix(filename, ".sql") {
		return false
	}
	if !strings.HasSuffix(filename, "_rollback.sql") {
		return false
	}

	if _, err := getId(filename); err != nil {
		return false
	}

	return true
}

func needsMigration(ctx context.Context, db *sql.DB, id int) (bool, error) {
	var hits int
	err := db.QueryRowContext(ctx, "SELECT COUNT(id) FROM "+metadataTableName+" WHERE id=$1", id).Scan(&hits)
	if err != nil || hits >= 1 {
		return false, err
	}

	return true, err
}
