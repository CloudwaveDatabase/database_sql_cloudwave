package common

import (
	"database/sql"
	"errors"
	_ "proxy.cloudwave.cn/share/go-sql-driver/cloudwave"
	"time"
)

type CloudConfigConn struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     string `json:"port"`
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

/**
* @Author junfenghe
* @Description 获取数据库连接
* @Date 2021-10-22 19:03
* @Param
* @return
**/
type ReqInitCloudwaveFull struct {
	Schema string `json:"schema"`
}

func InitCloudwaveFull(interfaceReqInitCloudwaveFull interface{}) (i *sql.DB, err error) {
	var reqInitCloudwaveFull ReqInitCloudwaveFull
	switch interfaceReqInitCloudwaveFull.(type) {
	case ReqInitCloudwaveFull:
		reqInitCloudwaveFull = interfaceReqInitCloudwaveFull.(ReqInitCloudwaveFull)
	default:
		return nil, errors.New("interfaceReqInitCloudwaveFull is not ReqInitCloudwaveFull")
	}
	dsn := "system:CHANGEME@(127.0.0.1:1978)/" + reqInitCloudwaveFull.Schema
	db, err := sql.Open("cloudwave", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	return db, nil
}

func InitMysql() (i *sql.DB, err error) {
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
