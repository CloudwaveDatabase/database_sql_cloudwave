package common

import (
	_ "cloudwave"
	"database/sql"
	_ "cloudwave"
	"time"
)

type CloudConfigConn struct {
	User string `json:"user"`
	Password string `json:"password"`
	Host string `json:"host"`
	Port string `json:"port"`
}
/**
* @Author junfenghe
* @Description 获取数据库连接
* @Date 2021-10-22 19:03
* @Param
* @return
**/
func InitCloudwave() (i *sql.DB, err error) {
	dsn := "system:CHANGEME@(127.0.0.1:1978)/tpch1"
	db, err := sql.Open("cloudwave", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	return db, nil
}

func InitMysql()  (i *sql.DB, err error)  {
	dsn := "root:cloudwave1@(82.156.106.27:30307)/data_from_cbbpa"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	return db, nil
}