package jsqlite_test

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	jsqlite "github.com/fujiwara/go-jsqlite"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	proxy "github.com/shogo82148/go-sql-proxy"
)

func init() {
	db, _ := sql.Open("sqlite3", "")
	sql.Register("sqlite3:proxy", proxy.NewProxyContext(db.Driver()))
	db.Close()
}

var selectTestSuite = []struct {
	query string
	rows  int
	json  string
}{
	{
		query: `select time, coalesce(message, json_extract(nested, '$.message')) as message from records where tag is null`,
		rows:  6,
	},
	{
		query: `SELECT true as matched FROM records WHERE message LIKE '%ERROR%'`,
		rows:  3,
	},
	{
		query: `SELECT xxx FROM records`,
		rows:  0,
	},
	{
		query: `SELECT * FROM records WHERE user_id > 1 AND is_bot`,
		rows:  1,
	},
	{
		query: `SELECT * FROM records WHERE user_id > 23456 AND is_bot`,
		rows:  1,
	},
	{
		query: `SELECT avg(reqtime) as avg_reqtime, sum(size) as total_size FROM records WHERE tag='access' and uri='/'`,
		rows:  1,
		json:  `[{"avg_reqtime":0.172,"total_size":666}]`,
	},
	{
		query: `SELECT req_id FROM records WHERE tag='katsubushi' order by req_id`,
		rows:  2,
		json:  `[{"req_id":12345},{"req_id":729653122850365440}]`,
	},
}

func TestRead(t *testing.T) {
	f, err := os.Open("tests/logs.json")
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	os.Remove("tests/test.sqlite")
	db, err := sql.Open("sqlite3:proxy", "file:tests/test.sqlite")
	if err != nil {
		t.Error(err)
	}

	runner := jsqlite.NewWithDB(sqlx.NewDb(db, "sqlite3"))
	if err := runner.Read(f); err != nil {
		t.Error(err)
	}
	defer runner.Close()
	for _, ts := range selectTestSuite {
		rows, err := runner.Select(ts.query)
		if err != nil && !jsqlite.NoSuchColumnError(err) {
			t.Error(err)
		}
		if len(rows) != ts.rows {
			t.Errorf("unexpected result rows %d expected %d", len(rows), ts.rows)
		}
		if ts.json != "" {
			if js := marshalJSON(rows); js != ts.json {
				t.Errorf("unexpected result json got %s expected %s", js, ts.json)
			}
		}
		t.Log(marshalJSON(rows))
	}
}

func marshalJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
