package a_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"os"
	_ "proxy.cloudwave.cn/share/go-sql-driver/cloudwave"
	"testing"
	"time"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
		//panic (err)
	}
}

func OpenDB() (*sql.DB, error) {
	var db *sql.DB
	var err error
	// db, err = sql.Open("cloudwave", "用户名:密码@IP:端口)/数据库")
	//	db, err = sql.Open("cloudwave", "system:CHANGEME@(127.0.0.1:1978)/itest")
	db, err = sql.Open("cloudwave", "system:CHANGEME@(127.0.0.1:1978)/test")
	if err != nil {
		panic(err)
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		panic(err)
		defer db.Close()
		return nil, err
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	return db, err
}

func InsertDB1() {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	// 执行插入记录语句
	result, err := db.Exec("insert into itest.userinfo values(10,'张三','上海', ?)", time.Now())
	checkErr(err)
	rowsAffected, _ := result.RowsAffected()
	fmt.Println("INSERT rowsAffected = ", rowsAffected)
}

func InsertDB2() {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	// 准备语句
	stmt, err := db.Prepare("insert into itest.userinfo values(?, ?, ?, ?)")
	checkErr(err)
	// defer关闭Prepare方法准备的语句
	defer stmt.Close()
	// 插入各记录
	res, err := stmt.Exec(11, "李四", "北京", time.Now())
	checkErr(err)
	id, err := res.LastInsertId()
	fmt.Println("INSERT dataID = ", id)
	res, err = stmt.Exec(12, "王五", "广州", time.Now())
	checkErr(err)
	id, err = res.LastInsertId()
	fmt.Println("INSERT dataID = ", id)
}

func InsertDB3() {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	// 准备语句
	stmt, err := db.Prepare("insert into itest.userinfo values(?, ?, ?, ?)")
	checkErr(err)
	// defer关闭Prepare方法准备的语句
	defer stmt.Close()
	// 利用循环插入多条记录
	i := 100
	for i < 200 {
		// 插入数据
		res, err := stmt.Exec(i, "name", "addr", time.Now())
		checkErr(err)
		rowsAffected, err := res.RowsAffected()
		checkErr(err)
		if rowsAffected != 1 {
			fmt.Println("INSERT rowsAffected = ", rowsAffected)
		}
		i++
	}
}

func InsertDB4() {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	// 开始事务
	tx, err := db.Begin()
	checkErr(err)
	stmt, err := tx.Prepare("insert into itest.userinfo values(?, ?, ?, ?)")
	checkErr(err)
	// defer关闭Prepare方法准备的语句
	defer stmt.Close()
	i := 300
	for i < 400 {
		// 插入不同类型的值
		res, err := stmt.Exec(i, "name", "addr", time.Now())
		checkErr(err)
		rowsAffected, err := res.RowsAffected()
		checkErr(err)
		if rowsAffected != 1 {
			fmt.Println("INSERT rowsAffected = ", rowsAffected)
		}
		i++
	}
	if i >= 400 {
		tx.Commit()
	} else {
		tx.Rollback()
	}
}

func UpdateDB() {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	// 进行修改
	result, err := db.Exec("UPDATE itest.userinfo set addr = ? where id = ?", "天津", 10)
	checkErr(err)
	// 查看本次修改影响到多少条记录
	rowsAffected, err := result.RowsAffected()
	checkErr(err)
	fmt.Println("UPDATE rowsAffected = ", rowsAffected)
}

func SelectDB1() {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	nm := "张三"
	id := 0
	name := ""
	addr := ""
	created := ""
	// 即将得到的name值转换成s.String类型并存储到&s中
	err = db.QueryRow("select * from itest.userinfo where name=?", nm).Scan(&id, &name, &addr, &created)
	checkErr(err)
	fmt.Println("id = ", id)
	fmt.Println("name = ", name)
	fmt.Println("addr = ", addr)
	fmt.Println("created = ", created)
}

func SelectDB2() {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	// 查询数据
	rows, err := db.Query("select name, addr from itest.userinfo where id >= ?;", 10)
	checkErr(err)
	defer rows.Close()
	name := ""
	addr := ""
	// 扫描结果集
	for rows.Next() {
		// 接收每条记录的字段内容
		err = rows.Scan(&name, &addr)
		checkErr(err)
		fmt.Print("name = ", name)
		fmt.Println("   addr = ", addr)
	}
	if rows.Err() != nil {
		fmt.Println(err)
	}
}

func SelectDB3() {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	// 查询数据
	rows, err := db.Query("select * from ssb1.lineorder;")
	cols, err := rows.Columns()
	vals := make([]interface{}, len(cols))
	checkErr(err)
	defer rows.Close()
	// 扫描结果集
	for i, _ := range cols {
		vals[i] = new(sql.RawBytes)
	}
	for rows.Next() {
		// 接收每条记录的字段内容
		err = rows.Scan(vals...)
		checkErr(err)
		for i := 0; i < len(vals); i++ {
			var n32 int32
			var n64 int64
			var f32 float32
			var f64 float64
			var str string
			var buf []byte

			dd := driver.Value(vals[i])
			fmt.Print("vals[", i, "] = ")
			switch v := dd.(type) {
			case byte:
			case int:
				n32 = int32(v)
				fmt.Println(n32)
			case int32:
				n32 = v
				fmt.Println(n32)
			case int64:
				n64 = v
				fmt.Println(n64)
			case float32:
				f32 = v
				fmt.Println(f32)
			case float64:
				f64 = v
				fmt.Println(f64)
			case []byte:
				buf = v
				fmt.Println(buf)
			case string:
				str = v
				fmt.Println(str)
				//			case time.Time:
				//				fmt.Println((time(v))
			default:
				fmt.Println(v)
			}
		}
		fmt.Println()
	}
	if rows.Err() != nil {
		fmt.Println(err)
	}
}

func DeleteDB() {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	// 进行修改
	result, err := db.Exec("DELETE FROM itest.userinfo where id=?", 10)
	checkErr(err)
	// 查看本次删除影响到多少条记录
	rowsAffected, err := result.RowsAffected()
	checkErr(err)
	fmt.Println("DELETE rowsAffected = ", rowsAffected)
}

func InsertCLOB(data string) {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	// 执行插入记录语句
	//result, err := db.Exec("insert into test.userclob values(20,'张三','上海1234567890abcdefghi')")
	stmt, er := db.Prepare("INSERT INTO test.userclob(ID, NAME, LOGF) VALUES (?, ?, ?)")
	if er != nil {
		panic(err.Error())
	}
	//data1 := "12234567890"
	_, err = stmt.Exec(50, "QWER1", data)

	checkErr(err)
	//rowsAffected, _ := result.RowsAffected()
	//fmt.Println("INSERT rowsAffected = ", rowsAffected)
}

func InsertBLOB() {
	var data []byte
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	// 执行插入记录语句
	stmt, er := db.Prepare("INSERT INTO test.userblob(ID, NAME, LOGF) VALUES (?, ?, ?)")
	if er != nil {
		panic(err.Error())
	}
	data = []byte("92234567890")
	_, err = stmt.Exec(115, "QWER99", data)
	if err != nil {
		panic(err.Error())
	}
	defer stmt.Close()
	//result, err := db.Exec("insert into test.userblob values(20,'张三','上海1234567890abcdefghi')")
	checkErr(err)
	//rowsAffected, _ := result.RowsAffected()
	//fmt.Println("INSERT rowsAffected = ", rowsAffected)
}

func SelectCLOB() {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	id := 0
	name := ""
	clobf := ""
	// 即将得到的name值转换成s.String类型并存储到&s中
	err = db.QueryRow("select * from test.userclob where id=50").Scan(&id, &name, &clobf)
	checkErr(err)
	fmt.Println("id = ", id)
	fmt.Println("name = ", name)
	fmt.Println("clobf = ", clobf)
}

func SelectBLOB() {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	var lobf []byte
	id := 0
	name := ""
	// 即将得到的name值转换成s.String类型并存储到&s中
	err = db.QueryRow("select * from test.userblob where id=115").Scan(&id, &name, &lobf)
	checkErr(err)
	fmt.Println("id = ", id)
	fmt.Println("name = ", name)
	fmt.Println("lobf = ", lobf)
}

func SelectTime() {
	db, err := OpenDB()
	checkErr(err)
	// defer关闭数据库连接
	defer db.Close()
	// 查询数据
	rows, err := db.Query("select * from test.product;")
	cols, err := rows.Columns()
	vals := make([]interface{}, len(cols))
	checkErr(err)
	defer rows.Close()
	// 扫描结果集
	for i, _ := range cols {
		vals[i] = new(sql.RawBytes)
	}

	for rows.Next() {
		// 接收每条记录的字段内容
		err = rows.Scan(vals...)
		checkErr(err)
		for i := 0; i < len(vals); i++ {
			var n32 int32
			var n64 int64
			var f32 float32
			var f64 float64
			var str string
			var buf []byte

			dd := driver.Value(vals[i])
			fmt.Print("vals[", i, "] = ")
			switch v := dd.(type) {
			case byte:
			case int:
				n32 = int32(v)
				fmt.Println(n32)
			case int32:
				n32 = v
				fmt.Println(n32)
			case int64:
				n64 = v
				fmt.Println(n64)
			case float32:
				f32 = v
				fmt.Println(f32)
			case float64:
				f64 = v
				fmt.Println(f64)
			case []byte:
				buf = v
				fmt.Println(buf)
			case string:
				str = v
				fmt.Println(str)
				//			case time.Time:
				//				fmt.Println((time(v))
			default:
				fmt.Println(v)
			}
		}
		fmt.Println()
	}
	if rows.Err() != nil {
		fmt.Println(err)
	}
}

// /         main        //////////////////////////////////////////////////////////////
func TestA(t *testing.T) {

	f, err := os.Open("D:\\CloudWave\\新程序4.0\\wisdomdata\\out.txt")
	if err != nil {
		fmt.Println("read file fail", err)
	}
	defer f.Close()

	//fd, err := ioutil.ReadAll(f)
	//if err != nil {
	//	fmt.Println("read to fd fail", err)
	//}
	//fmt.Println(string(fd))

	//InsertCLOB(string(fd))
	//InsertCLOB("123qwe456uio7890zxcv")
	//InsertBLOB()
	//SelectTime()
	//SelectCLOB()
	SelectBLOB()
	//InsertDB1() //OK
	//	InsertDB2()
	//InsertDB3()
	//InsertDB4()
	//	UpdateDB() //OK
	//	SelectDB1() //OK
	//	SelectDB2() //OK
	//SelectDB3()
	//	DeleteDB() //OK
}
