package jsqlite

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const (
	DefaultDSN      = ":memory:"
	SQLiteMaxColumn = 2000
)

var (
	tableName    = "records"
	placeHolders []string
)

func init() {
	for i := 1; i <= SQLiteMaxColumn; i++ {
		placeHolders = append(placeHolders, fmt.Sprintf("$%d", i))
	}
}

// QueryRunner represents a query runner of jsqlite.
type QueryRunner struct {
	db   *sqlx.DB
	cols map[string]struct{}
}

// Table returns a SQLite table name.
func (r *QueryRunner) Table() string {
	return tableName
}

// Select selects from table by a SQL query.
func (r *QueryRunner) Select(q string) ([]map[string]interface{}, error) {
	res := make([]map[string]interface{}, 0)
	rows, err := r.db.Queryx(q)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		row := make(map[string]interface{}, 0)
		if err := rows.MapScan(row); err != nil {
			return nil, err
		}
		res = append(res, row)
	}
	return res, nil
}

// New creates a QueryRunner
func New(dsn string) (*QueryRunner, error) {
	db, err := sqlx.Connect("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	return NewWithDB(db), nil
}

// NewWithDB creates a QueryRunner with the *sqlx.DB
func NewWithDB(db *sqlx.DB) *QueryRunner {
	return &QueryRunner{
		db:   db,
		cols: make(map[string]struct{}),
	}
}

// Read creates a QueryRunner and read JSONL from io.Reader
func Read(r io.Reader) (*QueryRunner, error) {
	runner, err := New(DefaultDSN)
	if err != nil {
		return nil, err
	}
	return runner, runner.Read(r)
}

// Read reads JSONL via io.Reader, creates table on in-memory SQLite and inserts records.
func (r *QueryRunner) Read(src io.Reader) error {
	dec := json.NewDecoder(src)

	tx := r.db.MustBegin()
	defer tx.Rollback()
	for {
		row := make(map[string]interface{}, 100)
		if err := dec.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := r.manageTable(tx, row); err != nil {
			return err
		}
		if err := r.insert(tx, row); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// NoSuchColumnError detect error from SQLite which describe "no such column".
func NoSuchColumnError(err error) bool {
	return strings.HasPrefix(err.Error(), "no such column:")
}

func (r *QueryRunner) manageTable(tx *sqlx.Tx, row map[string]interface{}) error {
	if len(r.cols) == len(row)/2 {
		return nil
	}

	addCols := make([]string, 0)
	for col := range row {
		if _, ok := r.cols[col]; !ok {
			addCols = append(addCols, col)
			r.cols[col] = struct{}{}
		}
	}
	if len(addCols) == len(r.cols) {
		if err := r.createTable(tx); err != nil {
			return err
		}
	} else if len(addCols) > 0 {
		if err := r.alterTable(tx, addCols); err != nil {
			return err
		}
	}
	return nil
}

func (r *QueryRunner) createTable(tx *sqlx.Tx) error {
	var colDefs []string
	for col := range r.cols {
		colDefs = append(colDefs, `"`+col+`"`)
	}
	q := fmt.Sprintf(
		`CREATE TABLE %s(%s)`,
		r.Table(),
		strings.Join(colDefs, ", "),
	)
	if _, err := tx.Exec(q); err != nil {
		return err
	}
	return nil
}

func (r *QueryRunner) alterTable(tx *sqlx.Tx, cols []string) error {
	for _, col := range cols {
		q := fmt.Sprintf(
			`ALTER TABLE %s ADD "%s"`,
			r.Table(),
			col,
		)
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

func (r *QueryRunner) insert(tx *sqlx.Tx, row map[string]interface{}) error {
	values := make([]interface{}, 0, len(row)/2)
	var colDefs []string
	for col, v := range row {
		switch v.(type) {
		case nil, string, float64, bool:
			values = append(values, v)
		default:
			// structured
			b, _ := json.Marshal(v)
			values = append(values, string(b))
		}
		colDefs = append(colDefs, `"`+col+`"`)
	}
	q := fmt.Sprintf(
		"INSERT INTO %s(%s) VALUES (%s)",
		r.Table(),
		strings.Join(colDefs, ","),
		strings.Join(placeHolders[0:len(colDefs)], ","),
	)
	_, err := tx.Exec(q, values...)
	return err
}
