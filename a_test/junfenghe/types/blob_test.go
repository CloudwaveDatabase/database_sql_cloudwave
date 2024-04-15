package types

import (
	"fmt"
	"log"
	"os"
	"proxy.cloudwave.cn/share/go-sql-driver/cloudwave/a_test/junfenghe/common"
	"testing"
)

var mapName = struct {
	schema string
}{"itest_go"}

// tearup 代码
func TestMain(m *testing.M) {
	// 初始化数据库连接
	db, err := common.InitCloudwaveFull(common.ReqInitCloudwaveFull{Schema: ""})
	if err != nil {
		log.Fatalln(err)
	}
	exec, err := db.Exec("drop schema if exists " + mapName.schema)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("tearup ...")
	log.Println(exec)
	result, err := db.Exec("create schema " + mapName.schema)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(result)
	// ...

	// 运行测试
	code := m.Run()

	// 清理数据库连接
	// ...

	// 退出测试
	os.Exit(code)
}

func TestBlob(t *testing.T) {

	db, err := common.InitCloudwaveFull(common.ReqInitCloudwaveFull{Schema: mapName.schema})

	if err != nil {
		t.Error(err)
	}
	_, err = db.Exec(fmt.Sprintf("create table %s(id integer,c1 blob, primary key(id))", t.Name()))
	if err != nil {
		t.Error(err)
	}
	prepare, err := db.Prepare("insert into " + t.Name() + " values(?, ?)")
	if err != nil {
		t.Error(err)
	}
	exec, err := prepare.Exec(1, []byte("1234567890"))
	if err != nil {
		t.Error(err)
	}
	log.Println(exec)
	rows, err := db.Query("select * from " + t.Name())
	for rows.Next() {
		var id int
		var c1 []byte
		rows.Scan(&id, &c1)
		log.Println(id, string(c1))
	}

}
