package migo

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	idLength          = 100
	metadataTableName = "migo_metadata"
	migrationsFolder  = "migrations"
)

// MigrateCtx takes a fs.FS containing sql files and runs all .sql files
// not ending with _rollback.sql on the provided db.
// Migration files should begin with an integer, optionally followed by
// an underscore and a description, and end with .sql.
// Migration files must not end with _rollback.sql
func MigrateCtx(ctx context.Context, db *sql.DB, fsys fs.FS) error {
	err := createMetadataTable(ctx, db)
	if err != nil {
		return fmt.Errorf("could not create or check for metadata table: %w", err)
	}

	migrationFiles, err := fs.ReadDir(fsys, migrationsFolder)
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

			err = migrateFile(ctx, db, fsys, migrationFile.Name())
			if err != nil {
				fmt.Printf("Error migrating %v, error: %v\n", migrationFile.Name(), err)
			} else {
				fmt.Printf("Migrated %v\n", migrationFile.Name())
			}
		}
	}
	return nil
}

// Migrate is equivalent to calling MigrateCtx(context.TODO(), db, fsys)
func Migrate(db *sql.DB, fsys fs.FS) error {
	return MigrateCtx(context.TODO(), db, fsys)
}

// PurgeCtx deletes all migo specific information from the db, leaving previously run migrations intact
func PurgeCtx(ctx context.Context, db *sql.DB) error {
	return dropMetadataTable(ctx, db)
}

// Purge is equivalent to calling PurgeCtx(context.TODO(), db)
func Purge(db *sql.DB) error {
	return PurgeCtx(context.TODO(), db)
}

func addMetadataEntry(ctx context.Context, tx *sql.Tx, filename string) error {
	id, err := getId(filename)
	if err != nil {
		return err
	}
	if len(filename) > idLength {
		filename = filename[:idLength-1]
	}

	currentTimestamp := time.Now().Format(time.RFC3339)
	_, err = tx.ExecContext(ctx, "INSERT INTO "+metadataTableName+" (id, name, migrated_at) VALUES ($1, $2, $3)", id, filename, currentTimestamp)
	if err != nil {
		return err
	}

	return nil
}

func createMetadataTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+metadataTableName+" (id int PRIMARY KEY, name VARCHAR("+fmt.Sprint(idLength)+") UNIQUE, migrated_at VARCHAR(30))")
	return err
}

func dropMetadataTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+metadataTableName)
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

// file must be a regular file containing sql commands
func migrateFile(ctx context.Context, db *sql.DB, fsys fs.FS, filename string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	queryAsByteSlice, err := fs.ReadFile(fsys, path.Join(migrationsFolder, filename))
	if err != nil {
		return err
	}

	err = addMetadataEntry(ctx, tx, filename)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Fatalf("unable to rollback failed adding of metadata entries for file %v, error: %v", filename, rollbackErr)
		}
		return err
	}

	_, err = tx.ExecContext(ctx, string(queryAsByteSlice))
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Fatalf("unable to rollback failed migration for file %v, error: %v", filename, rollbackErr)
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func needsMigration(ctx context.Context, db *sql.DB, id int) (bool, error) {
	var hits int
	err := db.QueryRowContext(ctx, "SELECT COUNT(id) FROM "+metadataTableName+" WHERE id=$1", id).Scan(&hits)
	if err != nil || hits >= 1 {
		return false, err
	}

	return true, err
}
