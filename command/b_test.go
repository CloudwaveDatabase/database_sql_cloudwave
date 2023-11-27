package command

import (
	"database/sql"
	"testing"
	//	"errors"
	"fmt"
	"log"
	_ "proxy.cloudwave.cn/share/go-sql-driver/cloudwave"
	"time"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

///         main        //////////////////////////////////////////////////////////////
func TestB(t *testing.T) {
	dbw := DbWorker{
		Dsn: "system:CHANGEME@(127.0.0.1:1978)/toutiao", //本机翰云
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
	dest, err := dbw.GetInfoNoparamCommon(GetRuntimeReport)
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
