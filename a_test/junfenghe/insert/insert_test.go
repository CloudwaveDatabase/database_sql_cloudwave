package insert

import (
	"database/sql"
	_ "github.com/cloudwavedatabase/database_sql_cloudwave"
	"testing"
	"time"
)

/**
* @Author junfenghe
* @Description 测试查询语法
* @Date 2021-10-22 19:02
* @Param
* @return
**/
func TestInsert(t *testing.T) {
	dsn := "system:CHANGEME@(127.0.0.1:1978)/itest"
	db, err := sql.Open("cloudwave", dsn)
	if err != nil {
		t.Error(err)
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	_, err = db.Exec("drop schema if exists itest")
	if err != nil {
		t.Error(err)
	}
	_, err = db.Exec("create schema itest")
	if err != nil {
		t.Error(err)
	}

	_, err = db.Exec("create table t1(id int, name varchar(255))")
	if err != nil {
		t.Error(err)
	}

	stmt, err := db.Prepare("insert into t1(id, name) values(?, ?)")
	if err != nil {
		t.Error(err)
	}
	var datas []interface{}
	datas = append(datas, 1, "张三")
	_, err = stmt.Exec(datas...)
	if err != nil {
		t.Error(err)
	}

	var datasMulti []interface{}
	datasMulti = append(datasMulti, 2, "张三")
	datasMulti = append(datasMulti, 3, "张三")
	_, err = stmt.Exec(datasMulti...)
	if err != nil {
		t.Error(err)
	}

}
