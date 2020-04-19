package pgmigrate

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/table"
	"github.com/jmoiron/sqlx"

	// import pg lib
	_ "github.com/lib/pq"
)

// Migrator struct holds migration configuration
type Migrator struct {
	Conn         string // pg connection string
	Table        string // table to store applied migrations: default migrations
	MigrationDir string // relative directory holding the migrations: default migrations
}

// DefaultMigrator constructs a Migrator with default values
func DefaultMigrator(conn string) *Migrator {
	return &Migrator{
		Conn:         conn,
		Table:        "migrations",
		MigrationDir: "migrations",
	}
}

// Migrate executes migrations specified in the migration directory
func (m *Migrator) Migrate() error {
	db, err := sqlx.Connect("postgres", m.Conn)
	if err != nil {
		return err
	}
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"migration", "status"})
	defer db.Close()
	err = createMigrationsTableIfNotExists(db, m.Table)
	if err != nil {
		return err
	}
	files, err := getFiles(m.MigrationDir)
	if err != nil {
		return err
	}
	for _, file := range files {
		id := strings.Split(file, m.Table+"/")[1]
		if rowExists(db, "SELECT * FROM "+m.Table+" WHERE id = $1", id) {
			t.AppendRow(table.Row{id, "already applied"})
			continue
		}
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		fcontent, err := ioutil.ReadAll(f)
		if err != nil {
			return err
		}
		txn := db.MustBegin()
		_, err = txn.Exec(string(fcontent))
		if err != nil {
			err = txn.Rollback()
			return err
		}
		_, err = txn.Exec("INSERT INTO "+m.Table+" (id) VALUES ($1)", id)
		if err != nil {
			err = txn.Rollback()
			return err
		}
		txn.Commit()
		t.AppendRow(table.Row{id, "applied now"})
		f.Close()
	}
	t.Render()
	return nil
}

func getFiles(path string) ([]string, error) {
	var files []string

	base, err := filepath.Rel(filepath.Base("."), path)
	if err != nil {
		return files, err
	}
	err = filepath.Walk(base, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	return files, err

}

func rowExists(db *sqlx.DB, query string, args ...interface{}) bool {
	var exists bool
	query = fmt.Sprintf("SELECT exists (%s)", query)
	err := db.QueryRow(query, args...).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		log.Fatalf("error checking if row exists '%s' %v", args, err)
	}
	return exists
}

func createMigrationsTableIfNotExists(db *sqlx.DB, table string) error {
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS " + table + " (id VARCHAR PRIMARY KEY)")
	return err
}

// CreateMigration creates migration in the specified MigrationDir
// The migration created has the following format:
// <timestamptz>_<some-name>.pgsql
func (m *Migrator) CreateMigration(name string) error {
	if name == "" {
		return errors.New("missing migration name")
	}
	base, err := filepath.Rel(filepath.Base("."), m.MigrationDir)
	if err != nil {
		return err
	}
	filename := time.Now().Format(time.RFC3339Nano) + "_" + name + ".pgsql"
	f, err := os.OpenFile(base+"/"+filename, os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	fmt.Printf("created migration %s\n", filename)
	defer f.Close()
	return nil
}
