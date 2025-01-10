package command

import (
	"database/sql"
	"github.com/CloudwaveDatabase/database_sql_cloudwave"
	"testing"
	"time"

	//	"errors"
	"fmt"
	_ "github.com/CloudwaveDatabase/database_sql_cloudwave"
	"log"
)

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func dimension1() {
	var err error
	var token string

	cmds := []string{
		/* 1 */ "2023年的销售额是多少",
		/* 2 */ "2023年的销售额同比增长率是多少",
		/* 3 */ "2023年销售额下降了，你能帮我做归因分析吗？",
		"你能帮我找出销售额下降的主要原因吗？", // 错
		"分析一下2023增长的维度",
		/* 4 */ "2022年的销售额是多少",
		/* 5 */ "2021年的销售额相对2020年的销售额同比增长率是多少",
		/* 6 */ "你能帮我找出销售额上升的主要原因吗",
		/* 7 */ "去年的销售额是多少",
		/* 8 */ "去年各个产品的销售额分别是多少",
		/* 9 */ "2022年各个产品的销售额分别是多少",
		/* 10 */ "2021年销售额前三的产品和销售额",
		/* 11 */ "2023年每个季度和产品的销售额",
		/* 12 */ "2021年各个产品每个月的销售额",
		/* 13 */ "2021年各个产品每个月的销售额按产品排序",

		"2023年每个月份的销售额的同比增长率",
		"2023年每月的销售额的同比增长率",
		"2023年每个月份的销售额的环比增长率",

		"2023年4月的销售额环比增长率是多少",

		"显示系统所有的指标"}

	his := []string{"2024年总出口额",
		"2024年每个月的出口额"}

	ex := cloudwave.Expand{}
	err = ex.StreamingChatBegin("system:CHANGEME@(127.0.0.1:1978)/cretail")
	if err != nil {
		panic(err)
	}
	ex.StreamingChatType("dimension")

	err = ex.UseSchema("cretail")
	if err == nil {
		for i := 0; i < len(cmds); i++ {
			//time.Sleep(time.Second * 10)
			fmt.Println(cmds[i])
			token, err = ex.StreamingChat(his, cmds[i])
			for true {
				if token == cloudwave.END_OF_STREAMING_CHAT {
					break
				}
				fmt.Print(token)
				token, err = ex.NextStreamingChat()
			}
			fmt.Println()
		}
	}
	ex.StreamingChatEnd()
}

func dimension() {
	var err error
	var token string

	cmds := []string{
		"2020年以来，我国每年的出口分别是多少",
	}
	his := []string{}

	ex := cloudwave.Expand{}
	err = ex.StreamingChatBegin("system:CHANGEME@(127.0.0.1:1978)/customs")
	if err != nil {
		panic(err)
	}
	ex.StreamingChatType("agent")

	err = ex.UseSchema("customs")
	if err == nil {
		for i := 0; i < len(cmds); i++ {
			//time.Sleep(time.Second * 10)
			fmt.Println(cmds[i])
			token, err = ex.StreamingChat(his, cmds[i])
			for true {
				if token == cloudwave.END_OF_STREAMING_CHAT {
					break
				}
				fmt.Print(token)
				token, err = ex.NextStreamingChat()
			}
			token, err = ex.ChatResult()
			fmt.Println(token)
		}
	}
	ex.StreamingChatEnd()
}

func graphrag() {
	var err error
	var token string

	cmds := []string{
		/* 1 */ "中国上海的经济怎样？"}
	his := []string{"2024年总出口额",
		"2024年每个月的出口额"}

	ex := cloudwave.Expand{}
	err = ex.StreamingChatBegin("system:CHANGEME@(127.0.0.1:1978)/ECONOMY")
	if err != nil {
		panic(err)
	}
	ex.StreamingChatType("graphrag")

	err = ex.UseSchema("ECONOMY")
	if err == nil {
		for i := 0; i < len(cmds); i++ {
			//time.Sleep(time.Second * 10)
			fmt.Println(cmds[i])
			token, err = ex.StreamingChat(his, cmds[i])
			for true {
				if token == cloudwave.END_OF_STREAMING_CHAT {
					break
				}
				fmt.Print(token)
				token, err = ex.NextStreamingChat()
			}
			fmt.Println()
		}
	}
	ex.StreamingChatEnd()
}

func rag() {
	var err error
	var token string

	cmds := []string{
		/* 1 */ "中国上海的经济怎样？"}
	his := []string{"2024年总出口额",
		"2024年每个月的出口额"}

	ex := cloudwave.Expand{}
	err = ex.StreamingChatBegin("system:CHANGEME@(127.0.0.1:1978)/ECONOMY")
	if err != nil {
		panic(err)
	}
	ex.StreamingChatType("rag")

	err = ex.UseSchema("ECONOMY")
	if err == nil {
		for i := 0; i < len(cmds); i++ {
			//time.Sleep(time.Second * 10)
			fmt.Println(cmds[i])
			token, err = ex.StreamingChat(his, cmds[i])
			for true {
				if token == cloudwave.END_OF_STREAMING_CHAT {
					break
				}
				fmt.Print(token)
				token, err = ex.NextStreamingChat()
			}
			fmt.Println()
		}
	}
	ex.StreamingChatEnd()
}

func chatResult() {
	var err error
	var token string

	cmds := []string{
		"2024年总出口额",
		"2024年每个月的出口额"}
	//his := []string{"2024年总出口额",
	//	"2024年每个月的出口额"}

	ex := cloudwave.Expand{}
	err = ex.StreamingChatBegin("system:CHANGEME@(127.0.0.1:1978)/ECONOMY")
	if err != nil {
		panic(err)
	}
	ex.StreamingChatType("dimension")

	err = ex.UseSchema("customs")
	if err == nil {
		for i := 0; i < len(cmds); i++ {
			//time.Sleep(time.Second * 10)
			fmt.Println(cmds[i])
			token, err = ex.Chat(nil, cmds[i])
			fmt.Println(token)
			token, err = ex.ChatResult()
			fmt.Println(token)
		}
	}
	ex.StreamingChatEnd()
}

func queryKnowledge() {
	var err error
	var token string

	his := []string{"2024年总出口额",
		"2024年每个月的出口额"}

	ex := cloudwave.Expand{}
	err = ex.StreamingChatBegin("system:CHANGEME@(127.0.0.1:1978)/ECONOMY")
	if err != nil {
		panic(err)
	}

	err = ex.UseSchema("cmedicine")
	if err == nil {
		fmt.Println("失眠，能睡过去，容易醒")
		token, err = ex.QueryKnowledge(his, "失眠，能睡过去，容易醒")
		fmt.Println(token)
	}
	ex.StreamingChatEnd()
}

// /         main        //////////////////////////////////////////////////////////////
func TestB(t *testing.T) {
	var err error
	dimension()
	//graphrag()
	//rag()
	//chatResult()
	//queryKnowledge()
	return

	dbw := DbWorker{
		Dsn: "system:CHANGEME@(127.0.0.1:1978)/cretail", //本机翰云
	}

	dbw.Db, err = sql.Open("cloudwave", dbw.Dsn)
	if err != nil {
		panic(err)
		return
	}

	// See "Important settings" section.
	dbw.Db.SetConnMaxLifetime(time.Minute * 1)
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
