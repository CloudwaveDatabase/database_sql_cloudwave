package types

import (
	"github.com/cloudwavedatabase/database_sql_cloudwave/a_test/junfenghe/common"
	"testing"
)

func TestClob(t *testing.T) {
	db, err := common.InitCloudwaveFull(common.ReqInitCloudwaveFull{Schema: "itest"})
	if err != nil {
		t.Error(err)
	}
	//timeInt := time.Now().Unix()

	db.Exec("use itest")
	//db.Exec("create table test_clob" + strconv.Itoa(int(timeInt)) + " (id int, name varchar(255), content clob)")

	query, err := db.Query("select * from TEST_CLOB1701090990")
	if err != nil {
		t.Error(err)
	}
	for query.Next() {
		var id, name, content string
		query.Scan(&id, &name, &content)
		t.Log(id, name, content)
	}
}
