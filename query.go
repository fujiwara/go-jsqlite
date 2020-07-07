package jsqlite

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var nameRegxp = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
var tableName = "records"

// QueryRunner represents a query runner of jsqlite.
type QueryRunner struct {
	db *sqlx.DB
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

// Read reads JSONL via io.Reader, creates table on in-memory SQLite and inserts records.
func Read(r io.Reader) (*QueryRunner, error) {
	db, err := sqlx.Connect("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	runner := &QueryRunner{db: db}
	dec := json.NewDecoder(r)
	rows := make([]map[string]interface{}, 0)
	cols := make(map[string]interface{}, 0)
	for {
		row := make(map[string]interface{}, 0)
		if err := dec.Decode(&row); err != nil {
			break
		}
		for col := range row {
			if nameRegxp.MatchString(col) {
				cols[col] = struct{}{}
			}
		}
		rows = append(rows, row)
	}
	var colDefs []string
	for col := range cols {
		colDefs = append(colDefs, `"`+col+`"`)
	}
	q := fmt.Sprintf(
		`CREATE TABLE %s(%s)`,
		runner.Table(),
		strings.Join(colDefs, ", "),
	)
	if _, err := db.Exec(q); err != nil {
		return nil, err
	}

	tx := db.MustBegin()
	for _, row := range rows {
		cols := make([]string, 0, len(row))
		values := make([]interface{}, 0, len(row))
		placeHolders := make([]string, 0, len(row))
		for col, v := range row {
			if v == nil {
				continue
			}
			switch v.(type) {
			case string:
				values = append(values, v.(string))
			case float64:
				values = append(values, strconv.FormatFloat(v.(float64), 'f', -1, 64))
			case bool:
				values = append(values, strconv.FormatBool(v.(bool)))
			default:
				// structured
				b, _ := json.Marshal(v)
				values = append(values, string(b))
			}
			cols = append(cols, `"`+col+`"`)
			placeHolders = append(placeHolders, fmt.Sprintf("$%d", len(cols)))
		}
		q := fmt.Sprintf(
			"INSERT INTO %s(%s) VALUES (%s)",
			runner.Table(),
			strings.Join(cols, ","),
			strings.Join(placeHolders, ","),
		)
		if _, err := tx.Exec(q, values...); err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	return runner, tx.Commit()
}

// NoSuchColumnError detect error from SQLite which describe "no such column".
func NoSuchColumnError(err error) bool {
	return strings.HasPrefix(err.Error(), "no such column:")
}
