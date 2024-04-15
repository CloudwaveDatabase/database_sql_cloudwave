package command

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"proxy.cloudwave.cn/share/go-sql-driver/cloudwave"
	"sort"
	"strings"
	//	"database/sql/driver"
)

type DbWorker struct {
	Dsn string
	Db  *sql.DB
}

func convert(cmd commandType) int {
	n := -10000
	switch cmd {
	case GetResultTaskStatistics:
		n = cloudwave.RESULT_SET_GET_EXECUTION_STATISTICS
	case GetTableComment, GetTableNameList, GetViewNameList:
		n = cloudwave.DATABASE_META_DATA_GET_TABLES
	case GetTableColumns, GetTableDefinition:
		n = cloudwave.DATABASE_META_DATA_GET_COLUMNS

	case GetTabletData:
		n = cloudwave.GET_TABLET_RESULT_SET

	case GetTableDistribution, GetTableDistributionStatistics:
		n = cloudwave.DATABASE_META_DATA_GET_TABLETS
	case GetOnlineSessions:
		n = cloudwave.GET_ONLINE_USER
	case GetRunningSQL:
		n = cloudwave.GET_RUNNING_SQL
	case GetSQLStatistics:
		n = cloudwave.DATABASE_GET_SQL_STATISTICS
	case GetHistorySQLs:
		n = cloudwave.DATABASE_GET_SQL_HISTORYS
	case GetCloudwaveVersion:
		n = cloudwave.GET_SERVER_VERSION
	case GetUserPrivileges:
		n = cloudwave.DATABASE_META_DATA_GET_USER_PRIVILEGES
	case GetMasterServerList, GetTabletServerList, GetServerList, GetServerStatus:
		n = cloudwave.DATABASE_META_DATA_GET_SERVERS
	case GetSystemUtilization:
		n = cloudwave.DATABASE_META_DATA_GET_SYSTEM_UTILIZATION
	case GetMemorySize:
		n = cloudwave.DATABASE_META_DATA_GET_MEMORY_SIZE
	case GetNetworkStatus:
		n = cloudwave.DATABASE_META_DATA_GET_NETWORK_STATUS
	case GetDfsStatus:
		n = cloudwave.GET_INFO_FROM_HDFS
	case GetConfigOptions:
		n = cloudwave.GET_CONFIG_OPTIONS

	case GetServerLogger:
		n = cloudwave.GET_SERVER_LOGGER
	case GetProcessJstack:
		n = cloudwave.GET_THREAD_INFO
	case GetHealthDiagnostic:
		n = cloudwave.DATABASE_HEALTH_DIAGNOSTIC
	case GetSystemOverview:
		n = cloudwave.GET_SYSTEM_OVERVIEW
	case DoRestartServer:
		n = cloudwave.DATABASE_RESTART_SERVER
	case GetSchemaNameList:
		n = cloudwave.DATABASE_META_DATA_GET_SCHEMAS
	case GetUserNameList:
		n = cloudwave.DATABASE_META_DATA_GET_USERS

	case GetRuntimeReport:
		n = cloudwave.DATABASE_GET_RUNTIME_REPORT
	}
	return n
}

func readString(data []byte) (string, int, error) {
	bytes, _, n, err := cloudwave.ReadLengthEncodedString(data[0:])
	if err != nil {
		return "", n, err
	}
	str := string(bytes)
	return str, n, nil
}

func (db *DbWorker) GetInfoNoparamCommon(cmd commandType) (interface{}, error) {
	//	var rows *driver.Rows
	cd := convert(cmd)
	if cmd == GetOnlineSessions || cmd == GetCloudwaveVersion || cmd == GetDfsStatus ||
		cmd == GetConfigOptions || cmd == GetSystemOverview || cmd == GetRuntimeReport {
		var s string
		var n int
		resExec, err := db.Db.Exec("CloudWave", cd)
		if err != nil {
			return nil, err
		}
		i, _ := resExec.RowsAffected()
		buf := cloudwave.PullData(int(i))
		if buf == nil && len(buf) < 5 {
			return nil, errors.New("no result")
		}
		switch cmd {
		case GetOnlineSessions:
			var s1, s2, s3 string
			count := int(binary.BigEndian.Uint32(buf[1:]))
			if count < 0 {
				return nil, nil
			}
			pos := 5
			ss := make([][]string, count)
			index := 0
			for index < count {
				s1, n, err = readString(buf[pos:])
				if err != nil {
					break
				}
				pos += n
				s2, n, err = readString(buf[pos:])
				if err != nil {
					break
				}
				pos += n
				//				s3, n, err = readString(buf[pos:])
				//				if err != nil {
				//					break
				//				}
				//				pos += n
				ss[index] = append(ss[index], s1, s2, s3)
				index++
			}
			return ss[0:index], err
		case GetCloudwaveVersion:
			var str string
			pos := 1
			str, n, err = readString(buf[pos:])
			if err != nil {
				break
			}
			pos += n
			s, n, err = readString(buf[pos:])
			if err != nil {
				break
			}
			str += (", " + s)
			return str, nil
		case GetDfsStatus, GetConfigOptions, GetSystemOverview:
			count := int(binary.BigEndian.Uint32(buf[1:]))
			if count < 0 {
				return nil, nil
			}
			pos := 5
			ss := make([]string, count)
			index := 0
			for index < count {
				s, n, err = readString(buf[pos:])
				if err != nil {
					break
				}
				pos += n
				ss[index] = s
				index++
			}
			return ss[0:index], err
		case GetRuntimeReport:
			str, _, err := readString(buf[1:])
			return str, err
		}
		return buf, nil
	}
	if cmd == GetRunningSQL || cmd == GetMasterServerList || cmd == GetTabletServerList ||
		cmd == GetServerList || cmd == GetServerStatus || cmd == GetSystemUtilization ||
		cmd == GetMemorySize || cmd == GetNetworkStatus || cmd == GetUserNameList {
		var index int
		rows, err := db.Db.Query("CloudWave", cd)
		defer rows.Close()
		if err != nil {
			return nil, err
		}
		switch cmd {
		case GetRunningSQL:
			var s1, s2, s3, s4, s5, s6, s7 string
			sss := make([][]string, MaxResultReturnRecord)
			sss[0] = append(sss[0], "发起用户", "发起时间", "SQL语句", "运行状态", "会话时戳", "会话序号", "句柄序号")
			index = 1
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7)
				if err != nil {
					return nil, err
				}
				sss[index] = append(sss[index], s1, s2, s3, s4, s5, s6, s7)
				index++
			}
			sss = sss[0:index]
			return sss, nil
		case GetMasterServerList, GetTabletServerList, GetServerList:
			var s1, s2, s3, s4, s5, s6, s7 string
			var i1, i2, i3 int64
			ss := make([]string, MaxResultReturnRecord)
			index = 0
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &i1, &i2, &i3, &s7)
				if err != nil {
					return nil, err
				}
				switch cmd {
				case GetMasterServerList:
					s2 = strings.ToLower(s2)
					s3 = strings.ToLower(s3)
					if strings.Index(s3, "standby") < 0 && strings.Index(s3, "offline") < 0 &&
						strings.Index(s2, "tablet") < 0 {
						ss[index] = s1
						index++
					}
				case GetTabletServerList:
					s2 = strings.ToLower(s2)
					s3 = strings.ToLower(s3)
					if strings.Index(s3, "standby") < 0 && strings.Index(s3, "offline") < 0 &&
						strings.Index(s2, "master") < 0 {
						ss[index] = s1
						index++
					}
				case GetServerList:
					s3 = strings.ToLower(s3)
					if strings.Index(s3, "standby") < 0 && strings.Index(s3, "offline") < 0 {
						ss[index] = s1
						index++
					}
				}
			}
			return ss[0:index], nil
		case GetServerStatus:
			var s1, s2, s3, s4, s5, s6, s7, s8, s9, s10 string
			sss := make([][]string, MaxResultReturnRecord)
			sss[0] = append(sss[0], "服务器", "类型", "状态", "JAVA", "系统", "处理器", "核心", "内存", "磁盘", "系统时间")
			index = 1
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10)
				if err != nil {
					return nil, err
				}
				sss[index] = append(sss[index], s1, s2, s3, s4, s5, s6, s7, s8, s9, s10)
				index++
			}
			sss = sss[0:index]
			return sss, nil
		case GetSystemUtilization:
			var s1, s2, s3, s4, s5 string
			sss := make([][]string, MaxResultReturnRecord)
			sss[0] = append(sss[0], "服务器", "类型", "状态", "系统利用率", "进程利用率")
			index = 1
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2, &s3, &s4, &s5)
				if err != nil {
					return nil, err
				}
				sss[index] = append(sss[index], s1, s2, s3, s4, s5)
				index++
			}
			sss = sss[0:index]
			return sss, nil
		case GetMemorySize:
			var s1, s2, s3, s4, s5, s6, s7, s8 string
			sss := make([][]string, MaxResultReturnRecord)
			sss[0] = append(sss[0], "服务器", "类型", "状态", "物理内存", "配置内存", "提交内存", "已用内存", "交换内存")
			index = 1
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8)
				if err != nil {
					return nil, err
				}
				sss[index] = append(sss[index], s1, s2, s3, s4, s5, s6, s7, s8)
				index++
			}
			sss = sss[0:index]
			return sss, nil
		case GetNetworkStatus:
			var s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14 string
			sss := make([][]string, MaxResultReturnRecord)
			sss[0] = append(sss[0], "服务器", "类型", "网卡", "时戳", "接收字节", "接收包", "接收错误",
				"接收丢包", "接收速率", "发送字节", "发送包", "发送错误", "发送丢包", "发送速率")
			index = 1
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10, &s11, &s12, &s13, &s14)
				if err != nil {
					return nil, err
				}
				sss[index] = append(sss[index], s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14)
				index++
			}
			sss = sss[0:index]
			return sss, nil
		case GetUserNameList:
			var s1, s2 string
			ss := make([]string, MaxResultReturnRecord)
			existDba := false
			index = 0
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2)
				if err != nil {
					return nil, err
				}

				if strings.Compare(s1, "system") == 0 {
					ss[index] = "管理员在尾部"
					existDba = true
				} else {
					ss[index] = s1
				}
				index++
			}
			ss = ss[0:index]
			if index > 1 {
				sort.Sort(sort.StringSlice(ss))
			}
			if existDba {
				ss[index-1] = "system"
			}
			return ss, nil
		}
		return nil, err
	}
	return nil, errors.New("command code is error")
}

// cmd :  getTableComment, getTableNameList, getViewNameList
func (db *DbWorker) GetNameList(cmd commandType, catalog string, schema []byte, table []byte) ([]string, error) {
	cd := convert(cmd)
	var js []byte
	switch cmd {
	case GetTableComment:
		js, _ = json.Marshal([]string{"TABLE"})
	case GetTableNameList:
		js, _ = json.Marshal([]string{"TABLE"})
	case GetViewNameList:
		js, _ = json.Marshal([]string{"VIEW"})
	}
	rows, err := db.Db.Query("CloudWave", cd, schema, table, json.RawMessage(js))
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4, s5 string
	ss := make([]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5)
		if err != nil {
			return nil, err
		}
		if cmd == GetTableComment {
			ss[index] = s5
		} else {
			ss[index] = s3
		}
		index++
	}
	return ss[0:index], nil
}

func (db *DbWorker) GetResultTaskStatistics(requestID int64) ([][]string, error) {
	cd := convert(GetResultTaskStatistics)
	resExec, err := db.Db.Exec("CloudWave", cd)
	if err != nil {
		return nil, err
	}
	i, _ := resExec.RowsAffected()
	buf := cloudwave.PullData(int(i))
	if buf == nil && len(buf) < 5 {
		return nil, errors.New("no result")
	}
	count := int(binary.BigEndian.Uint32(buf[1:]))
	pos := 5
	sss := make([][]string, count)
	var s string
	var n int
	index := 0
	for index < count {
		subcount := int(binary.BigEndian.Uint32(buf[pos:]))
		pos += 4
		for i := 0; i < subcount; i++ {
			s, n, err = readString(buf[pos:])
			if err != nil {
				break
			}
			pos += n
			sss[index] = append(sss[index], s)

		}
		index++
	}
	return sss[0:index], err
}

func (db *DbWorker) GetTableColumns(schema []byte, table []byte, requestID int64) ([][]string, error) {
	cd := convert(GetTableColumns)
	rows, err := db.Db.Query("CloudWave", cd, schema, table, []byte(nil))
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4, s5, s6, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22, s23, s24, s25 string
	var n7, n8, n9, n10 int64
	sss := make([][]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &n7, &n8, &n9, &n10, &s11, &s12,
			&s13, &s14, &s15, &s16, &s17, &s18, &s19, &s20, &s21, &s22, &s23, &s24, &s25)
		if err != nil {
			return nil, err
		}
		sss[index] = append(sss[index], s1, s2, s3, s4, s5, s6, fmt.Sprint(n7), fmt.Sprint(n8), fmt.Sprint(n9), fmt.Sprint(n10),
			s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22, s23, s24, s25)
		index++
		if index >= MaxResultReturnRecord {
			break
		}
	}
	sss = sss[0:index]
	return sss, nil
}

func (db *DbWorker) GetTableDefinition(schema []byte, table []byte, requestID int64) ([][]string, error) {
	cd := convert(GetTableDefinition)
	rows, err := db.Db.Query("CloudWave", cd, schema, table, []byte(nil))
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4, s5, s6, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22, s23, s24, s25 string
	var n7, n8, n9, n10 int64
	sss := make([][]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &n7, &n8, &n9, &n10, &s11, &s12,
			&s13, &s14, &s15, &s16, &s17, &s18, &s19, &s20, &s21, &s22, &s23, &s24, &s25)
		if err != nil {
			return nil, err
		}
		sss[index] = append(sss[index], s1, s2, s3, s4, s5, s6, fmt.Sprint(n7), fmt.Sprint(n8), fmt.Sprint(n9), fmt.Sprint(n10),
			s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22, s23, s24, s25)
		index++
		if index >= MaxResultReturnRecord {
			break
		}
	}
	sss = sss[0:index]
	return sss, nil
}

func (db *DbWorker) GetTabletData(schema string, table string, tabletID int64, requestID int64) ([][]string, error) {
	cd := convert(GetTabletData)
	rows, err := db.Db.Query("CloudWave", cd, schema, table, tabletID)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14 string
	sss := make([][]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10, &s11, &s12, &s13, &s14)
		if err != nil {
			return nil, err
		}
		sss[index] = append(sss[index], s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14)
		index++
		if index >= MaxResultReturnRecord {
			break
		}
	}
	sss = sss[0:index]
	return sss, nil
}

func (db *DbWorker) GetTableDistribution(schema string, table string, requestID int64) ([][]string, error) {
	cd := convert(GetTableDistribution)
	rows, err := db.Db.Query("CloudWave", cd, schema, table, requestID)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4 string
	sss := make([][]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4)
		if err != nil {
			return nil, err
		}
		sss[index] = append(sss[index], s1, s2, s3, s4)
		index++
		if index >= MaxResultReturnRecord {
			break
		}
	}
	sss = sss[0:index]
	return sss, nil
}

func (db *DbWorker) GetTableDistributionStatistics(schema string, table string) ([][]string, error) {
	cd := convert(GetTableDistributionStatistics)
	rows, err := db.Db.Query("CloudWave", cd, schema, table)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4 string
	sss := make([][]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4)
		if err != nil {
			return nil, err
		}
		sss[index] = append(sss[index], s1, s2, s3, s4)
		index++
		if index >= MaxResultReturnRecord {
			break
		}
	}
	sss = sss[0:index]
	return sss, nil
}

func (db *DbWorker) GetSQLStatistics(timeRange int64) (string, error) {
	cd := convert(GetSQLStatistics)
	resExec, err := db.Db.Exec("CloudWave", cd, timeRange)
	if err != nil {
		return "", err
	}
	i, _ := resExec.RowsAffected()
	buf := cloudwave.PullData(int(i))
	if len(buf) < 5 {
		return "", errors.New("result is null")
	}
	str, _, err := readString(buf[1:])
	return str, nil
}

func (db *DbWorker) getSQLHistorys(tp int, count int) ([]string, error) {
	cd := convert(GetHistorySQLs)
	resExec, err := db.Db.Exec("CloudWave", cd, float64(tp), float64(count))
	if err != nil {
		return nil, err
	}
	i, _ := resExec.RowsAffected()
	buf := cloudwave.PullData(int(i))
	if buf == nil && len(buf) < 5 {
		return nil, errors.New("no result")
	}
	count2 := int(binary.BigEndian.Uint32(buf[1:]))
	pos := 5
	ss := make([]string, count2)
	var s string
	var n int
	index := 0
	for index < count2 {
		s, n, err = readString(buf[pos:])
		if err != nil {
			break
		}
		pos += n
		ss[index] = s
		index++
	}
	return ss[0:index], err
}

func (db *DbWorker) GetHistorySQLs() ([][]string, error) {
	runSQLs, _ := db.getSQLHistorys(6, 1000)
	consumeSQLs, _ := db.getSQLHistorys(3, 10)
	failedSQLs, _ := db.getSQLHistorys(7, 10)
	recentSQLs, _ := db.getSQLHistorys(0, 1000)

	ss := make([][]string, 4)

	ss[0] = append(ss[0], "[Running SQLs]\n")
	for i := 0; i < len(runSQLs); i++ {
		ss[0] = append(ss[0], runSQLs[i]+"\n")
	}
	ss[0] = append(ss[0], "\n")

	ss[1] = append(ss[1], "[Consume SQLs]\n")
	for i := 0; i < len(consumeSQLs); i++ {
		ss[1] = append(ss[1], consumeSQLs[i]+"\n")
	}
	ss[1] = append(ss[1], "\n")

	ss[2] = append(ss[2], "[Failed SQLs]\n")
	for i := 0; i < len(failedSQLs); i++ {
		ss[2] = append(ss[2], failedSQLs[i]+"\n")
	}
	ss[2] = append(ss[2], "\n")

	ss[3] = append(ss[3], "[Recent SQLs]\n")
	for i := 0; i < len(recentSQLs); i++ {
		ss[3] = append(ss[3], recentSQLs[i]+"\n")
	}
	ss[3] = append(ss[3], "\n")
	return ss, nil
}

func (db *DbWorker) GetUserPrivileges(user string) (string, error) {
	cd := convert(GetUserPrivileges)
	resExec, err := db.Db.Exec("CloudWave", cd, user)
	if err != nil {
		return "", err
	}
	i, _ := resExec.RowsAffected()
	buf := cloudwave.PullData(int(i))
	if len(buf) < 5 {
		return "", errors.New("result is null")
	}
	str, _, err := readString(buf[1:])
	return str, nil
}

func (db *DbWorker) GetServerLogger(server []byte, tail bool, count int) (string, error) {
	cd := convert(GetServerLogger)
	resExec, err := db.Db.Exec("CloudWave", cd, server, tail, float64(count))
	if err != nil {
		return "", err
	}
	i, _ := resExec.RowsAffected()
	buf := cloudwave.PullData(int(i))
	if len(buf) < 5 {
		return "", errors.New("result is null")
	}
	str, _, err := readString(buf[1:])
	return str, err
}

func (db *DbWorker) GetProcessJstack(trim bool, server []byte) (string, error) {
	cd := convert(GetProcessJstack)
	resExec, err := db.Db.Exec("CloudWave", cd, trim, server)
	if err != nil {
		return "", err
	}
	i, _ := resExec.RowsAffected()
	buf := cloudwave.PullData(int(i))
	if len(buf) < 5 {
		return "", errors.New("result is null")
	}
	str, _, err := readString(buf[1:])
	return str, err
}

func (db *DbWorker) GetHealthDiagnostic(simpleCheck bool) ([]string, error) {
	cd := convert(GetHealthDiagnostic)
	resExec, err := db.Db.Exec("CloudWave", cd, simpleCheck)
	if err != nil {
		return nil, err
	}
	i, _ := resExec.RowsAffected()
	buf := cloudwave.PullData(int(i))
	if len(buf) < 5 {
		return nil, errors.New("result is null")
	}
	ss := make([]string, MaxResultReturnRecord)
	index := 0
	pos := 1
	for i := 0; i < 7; i++ {
		if buf[pos] == 0 {
			count := int(binary.BigEndian.Uint32(buf[pos:]))
			pos += 4
			for j := 0; j < count; j++ {
				checkType := int(binary.BigEndian.Uint32(buf[pos:]))
				pos += 4
				checkTarget := int(binary.BigEndian.Uint32(buf[pos:]))
				pos += 4
				healthServer, n, _ := readString(buf[pos:])
				pos += n
				healthWeight := float64(binary.BigEndian.Uint64(buf[pos:]))
				pos += 8
				healthScore := float64(binary.BigEndian.Uint64(buf[pos:]))
				pos += 8
				size := int(binary.BigEndian.Uint32(buf[pos:]))
				pos += 4
				fmt.Print(checkType)
				fmt.Print(checkTarget)
				fmt.Print(healthServer)
				fmt.Print(healthWeight)
				fmt.Print(healthScore)
				fmt.Println(size)
			}
		}
	}
	return ss[0:index], err
}

func (db *DbWorker) DoRestartServer(target string) (bool, error) {
	cd := convert(DoRestartServer)
	resExec, err := db.Db.Exec("CloudWave", cd, target)
	if err != nil {
		return false, err
	}
	i, _ := resExec.RowsAffected()
	cloudwave.PullData(int(i))
	return true, nil
}

func (db *DbWorker) GetSchemaNameList(catalog string) ([]string, error) {
	cd := convert(GetSchemaNameList)
	var b []byte
	b = nil
	rows, err := db.Db.Query("CloudWave", cd, b)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s string
	ss := make([]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&ss[index], &s)
		if err != nil {
			return nil, err
		}
		index++
	}
	return ss[0:index], nil
}
