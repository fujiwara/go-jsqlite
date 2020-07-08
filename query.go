package jsqlite

import (
	"bufio"
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
	bufSize      = 64 * 1024
)

func init() {
	for i := 1; i <= SQLiteMaxColumn; i++ {
		placeHolders = append(placeHolders, fmt.Sprintf("$%d", i))
	}
}

// QueryRunner represents a query runner of jsqlite.
type QueryRunner struct {
	db        *sqlx.DB
	cols      []string
	colsSet   map[string]struct{}
	colsDef   string
	stmtCache map[string]*sqlx.Stmt
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
		db:        db,
		colsSet:   make(map[string]struct{}),
		stmtCache: make(map[string]*sqlx.Stmt),
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
	switch src.(type) {
	case *bufio.Reader:
	default:
		src = bufio.NewReaderSize(src, bufSize)
	}
	dec := json.NewDecoder(src)
	tx := r.db.MustBegin()
	defer tx.Rollback()
	defer func() {
		r.stmtCache = make(map[string]*sqlx.Stmt)
	}()
	var row map[string]interface{}
	for {
		row = make(map[string]interface{}, 100)
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
	adds := make([]string, 0)
	for col := range row {
		if _, exists := r.colsSet[col]; exists {
			continue
		}
		adds = append(adds, col)
	}
	if len(adds) == 0 {
		return nil
	}
	if len(r.cols) == 0 {
		if err := r.createTable(tx, adds); err != nil {
			return err
		}
	} else {
		if err := r.alterTable(tx, adds); err != nil {
			return err
		}
	}
	return nil
}

func (r *QueryRunner) createTable(tx *sqlx.Tx, cols []string) error {
	r.addCols(cols...)
	q := fmt.Sprintf(`CREATE TABLE %s(%s)`, r.Table(), r.colsDef)
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
		r.addCols(col)
	}
	return nil
}

func (r *QueryRunner) addCols(cols ...string) {
	for _, col := range cols {
		r.cols = append(r.cols, col)
		r.colsSet[col] = struct{}{}
		if r.colsDef != "" {
			r.colsDef += ","
		}
		r.colsDef += `"` + col + `"`
	}
}

func (r *QueryRunner) insert(tx *sqlx.Tx, row map[string]interface{}) error {
	values := make([]interface{}, 0, len(r.cols))
	for _, col := range r.cols {
		v := row[col]
		switch v.(type) {
		case nil, string, float64, bool:
			values = append(values, v)
		default:
			// structured
			b, _ := json.Marshal(v)
			values = append(values, string(b))
		}
	}
	q := fmt.Sprintf(
		"INSERT INTO %s(%s) VALUES (%s)",
		r.Table(),
		r.colsDef,
		strings.Join(placeHolders[0:len(values)], ","),
	)
	if stmt, err := r.prepare(tx, q); err != nil {
		return err
	} else {
		_, err := stmt.Exec(values...)
		return err
	}
}

func (r *QueryRunner) prepare(tx *sqlx.Tx, query string) (*sqlx.Stmt, error) {
	if stmt, exists := r.stmtCache[query]; exists {
		return stmt, nil
	}
	if stmt, err := tx.Preparex(query); err != nil {
		return nil, err
	} else {
		r.stmtCache[query] = stmt
		return stmt, nil
	}
}
