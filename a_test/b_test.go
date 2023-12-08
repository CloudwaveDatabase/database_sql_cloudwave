package a

import (
	"database/sql"
	_ "github.com/cloudwavedatabase/database_sql_cloudwave"
	"github.com/cloudwavedatabase/database_sql_cloudwave/command"
	"testing"
	//	"errors"
	"fmt"
	"log"
	"time"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

///         main        //////////////////////////////////////////////////////////////
func TestB(t *testing.T) {
	dbw := command.DbWorker{
		Dsn: "tpch1:tpch1@(127.0.0.1:1978)/tpch1", //本机翰云
	}
	var err error
	dbw.Db, err = sql.Open("cloudwave", dbw.Dsn)
	if err != nil {
		panic(err)
		return
	}
	// See "Important settings" section.
	dbw.Db.SetConnMaxLifetime(time.Minute * 3)
	dbw.Db.SetMaxOpenConns(10)
	dbw.Db.SetMaxIdleConns(10)

	//	dest, err := dbw.GetInfoNoparamCommon(GetNetworkStatus)
	dest, err := dbw.GetInfoNoparamCommon(command.GetRuntimeReport)
	if err == nil && dest != nil {
		switch v := dest.(type) {
		case []byte:
			buf := v
			fmt.Println(buf)
		case string:
			s := v
			fmt.Println(s)
		case []string:
			ss := v
			fmt.Println(ss)
		case [][]string:
			sss := v
			fmt.Println(sss)
		default:
		}
	}
	fmt.Println(dest)

	//	str, err := dbw.GetServerLogger("server", true, -10001000)
	//	fmt.Println(str)

	//	str, err := dbw.GetProcessJstack(false, "asdfg")
	//	fmt.Println(str)

	//	str, err = dbw.GetHealthDiagnostic(false)	//有待调试
	//	fmt.Println(str)

	//	b, err := dbw.DoRestartServer("ss")
	//	fmt.Println(b)

	//	ss, err := dbw.GetSchemaNameList("catalog")
	//	fmt.Println(ss)

	//	GetTableComment, GetTableNameList, GetViewNameList
	//	ss, err := dbw.GetNameList(GetTableComment, "catalog", []byte("toutiao"), []byte("table"))
	//	fmt.Println(ss)

	//	s, err := dbw.GetUserPrivileges("hu")
	//	fmt.Println(s)

	//	s, err := dbw.GetSQLStatistics(1000)
	//	fmt.Println(s)

	//	ss, err := dbw.GetTableDistribution("toutiao", "testtable", 0)
	//	fmt.Println(ss)

	//	ss, err := dbw.GetTableDistributionStatistics("toutiao", "testtable")
	//	fmt.Println(ss)

	//	ss, err := dbw.GetTabletData("toutiao", "testtable", 0, 0)
	//	fmt.Println(ss)

	//	ss, err := dbw.GetResultTaskStatistics(0)
	//	fmt.Println(ss)

	//	ss, err := dbw.GetTableColumns("toutiao", "testtable", 0)
	//	fmt.Println(ss)

	ss, err := dbw.GetHistorySQLs()
	fmt.Println(ss)

	dbw.Db.Close()
	fmt.Println("end")
}

func TestExec(t *testing.T) {

	dsn := "system:CHANGEME@(127.0.0.1:1978)/ssb1"
	db, err := sql.Open("cloudwave", dsn)
	if err != nil {
		t.Error(err)
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	rows, err := db.Query("select * from ssb1.lineorder")
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Error(err)
	}
	colsType, err := rows.ColumnTypes()
	if err != nil {
		t.Error(err)
	}
	t.Log(cols)
	t.Log(colsType)
	//defer rows.Close()
	//var regions []Region
	for rows.Next() {
		t.Log("123")
		//var t1, t2 string
		//rows.Scan(&t1, &t2)
		//t.Log(t1)
		//t.Log(t2)
	}
	t.Log("result: ", rows)
}
