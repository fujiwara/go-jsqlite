package jsqlite

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"

	_ "github.com/mattn/go-sqlite3" // db driver
)

const (
	sqliteMaxColumn = 2000
	maxFloat64      = 9007199254740991 // 2 ** 53 - 1
)

var (
	tableName    = "records"
	placeHolders []string
	bufSize      = 64 * 1024
)

func init() {
	for i := 1; i <= sqliteMaxColumn; i++ {
		placeHolders = append(placeHolders, fmt.Sprintf("$%d", i))
	}
}

func generateDSN(memory bool) (string, func()) {
	if memory {
		return ":memory:", func() {}
	}
	tmpfile, _ := ioutil.TempFile("", "jsqlite.*.db")
	return fmt.Sprintf("file:%s?_sync=off&_vacuum=none", tmpfile.Name()), func() {
		os.Remove(tmpfile.Name())
	}
}

// QueryRunner represents a query runner of jsqlite.
type QueryRunner struct {
	db        *sqlx.DB
	cols      []string
	colsSet   map[string]struct{}
	colsDef   string
	stmtCache map[string]*sqlx.Stmt
	cleanup   func()
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
func New(memory bool) (*QueryRunner, error) {
	dsn, cleanup := generateDSN(memory)
	db, err := sqlx.Connect("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	r := NewWithDB(db)
	r.cleanup = cleanup
	return r, nil
}

// NewWithDB creates a QueryRunner with the *sqlx.DB
func NewWithDB(db *sqlx.DB) *QueryRunner {
	return &QueryRunner{
		db:        db,
		colsSet:   make(map[string]struct{}),
		stmtCache: make(map[string]*sqlx.Stmt),
	}
}

// Close closes a database and cleanup the database file if nessesary.
func (r *QueryRunner) Close() error {
	if r.cleanup != nil {
		r.cleanup()
	}
	return r.db.Close()
}

// Read creates a QueryRunner and read JSONL from io.Reader
func Read(r io.Reader) (*QueryRunner, error) {
	runner, err := New(false)
	if err != nil {
		return nil, err
	}
	return runner, runner.Read(r)
}

// Read reads JSONL via io.Reader, creates table on in-memory SQLite and inserts records.
func (r *QueryRunner) Read(src io.Reader) error {
	return r.ReadWithContext(context.Background(), src)
}

// ReadWithContext reads JSONL via io.Reader, creates table on in-memory SQLite and inserts records with context.
func (r *QueryRunner) ReadWithContext(ctx context.Context, src io.Reader) error {
	ch := make(chan map[string]interface{}, 1000)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.readWorker(ch, src)
	})
	g.Go(func() error {
		return r.loadWorker(ch)
	})
	return g.Wait()
}

func (r *QueryRunner) readWorker(ch chan map[string]interface{}, src io.Reader) error {
	defer close(ch)

	switch src.(type) {
	case *bufio.Reader:
	default:
		src = bufio.NewReaderSize(src, bufSize)
	}
	dec := json.NewDecoder(src)
	dec.UseNumber()
	var row map[string]interface{}
	for {
		row = make(map[string]interface{}, 100)
		if err := dec.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		ch <- row
	}
	return nil
}

func (r *QueryRunner) loadWorker(ch chan map[string]interface{}) error {
	defer func() {
		r.stmtCache = make(map[string]*sqlx.Stmt)
	}()

	tx := r.db.MustBegin()
	defer tx.Rollback()
	for row := range ch {
		if err := r.manageTable(tx, row); err != nil {
			return err
		}
		if err := r.insert(tx, row); err != nil {
			return err
		}
	}
	tx.Commit()
	return nil
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

func toValue(in interface{}) interface{} {
	switch v := in.(type) {
	case nil, string, []byte, float64, bool:
		return v
	case json.Number:
		if fv, err := v.Float64(); err != nil {
			return v.String()
		} else if fv <= maxFloat64 {
			return fv
		} else {
			iv, err := v.Int64()
			if err != nil {
				return v.String()
			}
			return iv
		}
	default:
		// structured
		b, _ := json.Marshal(v)
		return string(b)
	}
}

func (r *QueryRunner) insert(tx *sqlx.Tx, row map[string]interface{}) error {
	values := make([]interface{}, 0, len(r.cols))
	for _, col := range r.cols {
		values = append(values, toValue(row[col]))
	}
	q := fmt.Sprintf(
		"INSERT INTO %s(%s) VALUES (%s)",
		r.Table(),
		r.colsDef,
		strings.Join(placeHolders[0:len(values)], ","),
	)
	stmt, err := r.prepare(tx, q)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(values...)
	return err
}

func (r *QueryRunner) prepare(tx *sqlx.Tx, query string) (*sqlx.Stmt, error) {
	if stmt, exists := r.stmtCache[query]; exists {
		return stmt, nil
	}
	stmt, err := tx.Preparex(query)
	if err != nil {
		return nil, err
	}
	r.stmtCache[query] = stmt
	return stmt, nil
}
