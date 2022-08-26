package a_test

import (
	"database/sql"
	//	"errors"
	"fmt"
	_ "cloudwave"
	"log"
	"testing"
	"time"
)

func checkErr(err error){
	if err != nil{
		log.Fatal(err)
	}
}

type DbWorker struct {
	Dsn      string
	Db       *sql.DB
}

//////////////////////////////////////////////////////////////////////////////////////
func (db *DbWorker) testTx() {
	tx, err := db.Db.Begin()
	checkErr(err)

	stmt1, err := tx.Prepare("insert into toutiao.testtable7 values(?, ?)")
	checkErr(err)
	result, err := stmt1.Exec(1118, -1235)
	checkErr(err)
	id, err := result.LastInsertId()
	fmt.Println("LastInsertId = ", id)

	stmt1.Close()

	tx.Commit()
//	tx.Rollback()


/*
	tx, err = db.Db.Begin()
	checkErr(err)

	stmt2, err := tx.Prepare("select col_long,col_char from toutiao.testtable2 where col_long=?")
	checkErr(err)

	id = 88
	//查询数据
	var ulong int64
	var uchar string
	err = stmt2.QueryRow(id).Scan(&ulong, &uchar)
	checkErr(err)
	fmt.Println("ulong = %ld, uchar is ", ulong, uchar)
	defer stmt2.Close()
*/
}

//////////////////////////////////////////////////////////////////////////////////////
func (db *DbWorker) testDbExec() {

	i := 100
//	result, err := db.Db.Exec("INSERT INTO toutiao.testtable  VALUES (2, 3, 4.5, 6.7, 8.9, true, 'a', 'bcd', ?, '2021-09-27 20:03:01', null, null)", "2021-07-01")
	result, err := db.Db.Exec("INSERT INTO toutiao.testtable4  VALUES (201, ?)", "2021-07-01")
	if err != nil {
	}
	i++
	result, err = db.Db.Exec("INSERT INTO toutiao.testtable3  VALUES (?, ?)", i, i * 10)
	if err != nil {
	}
	i++
	result, err = db.Db.Exec("INSERT INTO toutiao.testtable3  VALUES (?, ?)", i, i * 10)
	if err != nil {
	}
	i++

//	result, err := db.Exec("INSERT INTO toutiao.testtable2 (col_long, col_char) VALUES (?, ?)",3456,"yyabcdef")
//	if err != nil {
//	}

	result, err = db.Db.Exec("UPDATE toutiao.testtable3 set col_long = ? where col_int = ?",888, 1020)
//	result, err := db.Exec("UPDATE toutiao.testtable2 set col_long=5555 where col_char='rrrrrrr'")
	if err != nil {
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		checkErr(err)
	}
	fmt.Println("UPDATE rowsAffected = ", rowsAffected)

	result, err = db.Db.Exec("DELETE FROM toutiao.testtable3 where col_long=?", 100)
	if err != nil {
	}

	rowsAffected, err = result.RowsAffected()
	if err != nil {
		checkErr(err)
	}
	fmt.Println("DELETE rowsAffected = ", rowsAffected)

	println(result)
}

//////////////////////////////////////////////////////////////////////////////////////
func (db *DbWorker) testDbQuery() {
//	rows,err:=db.Db.Query("select name,period from exhibitor_production where name='医圣'")
//	rows,err:=db.Db.Query("select  from POSTID='505342300944138' or AREA='阿曼/阿联酋/卡塔尔'")
//	rows,err:=db.Db.Query("select AREA, photoid from tecno.facebook_posts where photoid='592769867868048' or photoid='592316527913382'")
//	rows,err:=db.Db.Query("select CATEGORY,CONTENT from tecno.news_table where content contains '上海'")
//	rows,err:=db.Db.Query("select CATEGORY,YEAR from toutiao.col_longcolint,news_table where ID = '104308';")

	i := 112
  	result, err := db.Db.Exec("INSERT INTO toutiao.testtable8  VALUES (?, ?)", i, "-1234567890123.1234567891")
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		checkErr(err)
	}
	fmt.Println("INSERT rowsAffected = ", rowsAffected)
	rows,err:=db.Db.Query("select col_long,col_number from toutiao.testtable8 where col_long = ?;", i)
	// 	toutiao.testtable
	if err != nil{
		panic(err)
	}
	defer rows.Close()
	c1 := 0
	c2 := ""
	for rows.Next(){
		err = rows.Scan(&c1, &c2)
		if err != nil{
			panic(err)
		}
		fmt.Println("C1 = ", c1)
		fmt.Println("C2 = ", c2)
	}
}

//////////////////////////////////////////////////////////////////////////////////////
func (db *DbWorker) testDbQueryRow() {
	dd := 101
	s1 := 0
	s2 := ""
	err := db.Db.QueryRow("select * from toutiao.testtable4 where col_long=?", dd).Scan(&s1, &s2) //即将得到的name值转换成s.String类型并存储到&s中
	if err != nil{
		panic(err)
	}

	fmt.Println("S1 = ", s1)
	fmt.Println("S2 = ", s2)
}

//////////////////////////////////////////////////////////////////////////////////////
func (db *DbWorker) testPrepareExec() {
//	stmt, err := db.Db.Prepare("insert into toutiao.testtable(col_integer,col_long,col_float,col_double,col_number,col_boolean,col_char,col_varchar,col_date,col_timestamp,col_binary,col_varbinary) values(2, ?, 4.5, 6.7, 8.9, true, ?, 'bcd', '2021-09-27', '2021-09-27 20:03:01', null, null)")
//	stmt, err := db.Db.Prepare("insert into toutiao.testtable2 values(?, ?)")
//	stmt, err := db.Db.Prepare("insert into toutiao.testtable2 values(?, ?)")	//OK
	stmt, err := db.Db.Prepare("insert into toutiao.testtable2 values(?, ?)")
	//	stmt, err := db.Prepare("update toutiao.testtable2 set col_char=? where col_long=?")
	if err != nil{
		panic(err)
	}
//	stmt.setAutoCommit(true)

	//执行准备好的Stmt
	res, err := stmt.Exec(123001, "asdfgh1")
	if err != nil{
		panic(err)
	}

	res, err = stmt.Exec(123002, "asdfgh2")
	if err != nil{
		panic(err)
	}

	res, err = stmt.Exec(123003, "asdfgh3")
	if err != nil{
		panic(err)
	}
	if res != nil {
	}
	time.Sleep(time.Second * 1)
	stmt.Close()

	stmt, err = db.Db.Prepare("delete from toutiao.testtable2 where col_long=?")
	res, err = stmt.Exec(123001)
	if err != nil{
		panic(err)
	}
//	stmt.mc.setAutoCommit(true)
//	res, err = stmt.Exec(123002)
//	if err != nil{
//		panic(err)
//	}
	res, err = stmt.Exec(123003)
	if err != nil{
		panic(err)
	}

/*
	id, err := res.LastInsertId()
	if err != nil{
		panic(err)
	}
	t.Log("ID : ", id)
*/
	time.Sleep(time.Second * 1)
	stmt.Close()


	time.Sleep(time.Second * 2)
}

func (db *DbWorker) testPrepareQuery() {
	stmt, err := db.Db.Prepare("select col_long, col_char from toutiao.testtable2 where col_long=?")
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	rows, err := stmt.Query(88)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	c1 := 0
	c2 := ""
	for rows.Next() {
		err = rows.Scan(&c1, &c2)
		if err != nil {
			fmt.Printf(err.Error())
			continue
		}
		fmt.Println("C1 = ", c1)
		fmt.Println("C2 = ", c2)
	}
}

func (db *DbWorker) testPrepare() {
}

///         main        //////////////////////////////////////////////////////////////
func TestA(t *testing.T) {

	dbw := DbWorker{
		Dsn: "system:CHANGEME@(127.0.0.1:1978)/toutiao",              //本机翰云
//		Dsn: "root:cloudwave1@(82.156.106.27:30307)/data_from_cbbpa", //服务器翰云
//		Dsn: "tecno:tecno@(82.156.106.27:1978)/tecno",                //服务器翰云
//		Dsn: "system:CHANGEME@(82.156.106.27:1978)/tecno",            //服务器翰云
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

	defer dbw.Db.Close()

//	err = db.Ping()
//	if err != nil {
//		panic(err)
//	}

//	dbw.testTx()			//OK
//	dbw.testDbExec()		//OK
	dbw.testDbQuery()		//OK
//	dbw.testDbQueryRow()	//OK
//	dbw.testPrepareExec()	//not OK
//	dbw.testPrepareQuery()	//OK

	t.Log("end")
}
