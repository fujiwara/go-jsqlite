package jsqlite_test

import (
	"encoding/json"
	"os"
	"testing"

	jsqlite "github.com/fujiwara/go-jsqlite"
)

var selectTestSuite = []struct {
	query string
	rows  int
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
}

func TestRead(t *testing.T) {
	f, err := os.Open("tests/logs.json")
	if err != nil {
		t.Error(err)
	}
	defer f.Close()
	runner, err := jsqlite.Read(f)
	if err != nil {
		t.Error(err)
	}
	for _, ts := range selectTestSuite {
		rows, err := runner.Select(ts.query)
		if err != nil && !jsqlite.NoSuchColumnError(err) {
			t.Error(err)
		}
		if len(rows) != ts.rows {
			t.Errorf("unexpected result rows %d expected %d", len(rows), ts.rows)
		}
		t.Log(marshalJSON(rows))
	}
}

func marshalJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
