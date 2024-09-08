package doc

import (
	"github.com/cloudwavedatabase/database_sql_cloudwave/a_test/junfenghe/common"
	"testing"
	"time"
)

func TestDoc_IF_EXISTS(t *testing.T) {
	db, err := common.InitCloudwave()
	if err != nil {
		t.Error(err)
	}
	_, err = db.Exec("DROP SCHEMA IF EXISTS itest")
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
}

type rsTypes struct {
	ID  int       `json:"id"`
	C1  int64     `json:"c1"`
	C2  int       `json:"c2"`
	C3  float64   `json:"c3"`
	C4  float64   `json:"c4"`
	c5  time.Time `json:"c5"`
	c6  time.Time `json:"c6"`
	c7  string    `json:"c7"`
	c8  bool      `json:"c8"`
	c9  []byte    `json:"c9"`
	c10 string    `json:"c10"`
}

func TestDoc_Types(t *testing.T) {
	db, err := common.InitCloudwave()
	if err != nil {
		t.Error(err)
	}
	_, err = db.Exec("DROP SCHEMA itest")
	if err != nil {
		t.Log(err)
	}

	_, err = db.Exec("CREATE SCHEMA itest")
	if err != nil {
		t.Error(err)
	}

	sqlText := "CREATE TABLE itest.test_cloudwave_types(id INTEGER, c1 LONG, c2 INT, c3 DOUBLE, c4 DECIMAL, c5 DATE , c6 DATETIME, c7 CLOB, c8 BOOLEAN, c9 BLOB, c10 JSON, PRIMARY KEY(id))"
	_, err = db.Exec(sqlText)
	if err != nil {

		t.Error(err)
	}

	//var rs rsTypes
	//rows, err := db.Query("SELECT * FROM itest.test_cloudwave_types")
	//if err != nil {
	//	t.Error(err)
	//}
	//rows.S
	t.Log(db)
}
