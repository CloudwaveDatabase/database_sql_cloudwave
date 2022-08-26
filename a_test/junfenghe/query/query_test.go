package query

import (
	_ "cloudwave"
	"cloudwave/a_test/junfenghe/common"
	"testing"
)

type Region struct {
	RRegionkey int    `json:"r_regionkey"`
	RName      string `json:"r_name"`
	RComment   string `json:"r_comment"`
}

/**
* @Author junfenghe
* @Description 测试查询语法
* @Date 2021-10-22 19:02
* @Param
* @return
**/
func TestQueryRow(t *testing.T) {
	db, err := common.InitCloudwave()
	if err != nil {
		t.Error(err)
	}
	var region Region
	err = db.QueryRow("select `r_regionkey`, `r_name`, `r_comment` from tpch1.region;").Scan(&region.RRegionkey, &region.RName, &region.RComment)
	if err != nil {
		t.Error(err)
	}
	t.Log(region)
}

/**
* @Author junfenghe
* @Description 测试查询语法
* @Date 2021-10-25 8:13
* @Param
* @return
**/
func TestQuery(t *testing.T) {
	db, err := common.InitCloudwave()
	if err != nil {
		t.Error(err)
	}
	//db.Exec("use schema tpch1")
	rows, err := db.Query("select `r_regionkey`, `r_name`, `r_comment` from tpch1.region where 1 = ?",1)
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()
	var regions []Region
	for rows.Next() {
		var region Region
		err = rows.Scan(&region.RRegionkey, &region.RName, &region.RComment)
		if err != nil {
			t.Error(err)
		}
		regions = append(regions, region)
	}
	t.Log("result: ", regions)
}

/**
* @Author junfenghe
* @Description
* @Date 2021-10-22 19:33
* @Param
* @return
**/
func TestInitCloudwave(t *testing.T) {
	db, err := common.InitCloudwave()
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
}

/**
* @Author junfenghe
* @Description test `QueryRow` using mysql
* @Date 2021-10-22 19:47
* @Param
* @return
**/

func TestQueryRowMysql(t *testing.T) {
	db, err := common.InitMysql()
	if err != nil {
		t.Error(err)
	}
	var name string
	err = db.QueryRow("select name from exhibitor_production").Scan(&name)
	if err != nil {
		t.Error(err)
	}
	t.Log(name)
}

/**
* @Author junfenghe
* @Description test `QueryRow` using cloudwave
* @Date 2021-10-22 19:47
* @Param
* @return
**/

func TestQueryRowMyCloudwave(t *testing.T) {
	db, err := common.InitCloudwave()
	if err != nil {
		t.Error(err)
	}
	var name string
	err = db.QueryRow("select name from exhibitor_production").Scan(&name)
	if err != nil {
		t.Error(err)
	}
	t.Log(name)
}
