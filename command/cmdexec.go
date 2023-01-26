package command

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"proxy.cloudwave.cn/share/go-sql-driver/cloudwave"
	"sort"
	"strconv"
	"strings"
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
	case GetTableComment, GetViewDefinition, GetTableNameList, GetViewNameList, GetTables, GetViews:
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

	case SaveConfigOptions:
		n = cloudwave.UPDATE_CONFIG_OPTIONS + 10000

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

	case GetPrimaryKeys:
		n = cloudwave.DATABASE_META_DATA_GET_PRIMARY_KEYS
	case GetUniqueKeys:
		n = cloudwave.DATABASE_META_DATA_GET_UNIQUE_KEYS
	case GetFullTextIndexColumns:
		n = cloudwave.GET_FULLTEXTINDEX_INFO
	case GetTextIndexColumns:
		n = cloudwave.GET_TEXTINDEX_INFO
	case GetIndexColumns:
		n = cloudwave.GET_INDEX_INFO
	case GetSchemas:
		n = cloudwave.DATABASE_META_DATA_GET_SCHEMAS
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
				s3, n, err = readString(buf[pos:])
				if err != nil {
					break
				}
				pos += n
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
			var s1, s2, s3, s4, s5, s6, s7 []byte
			sss := make([][]string, MaxResultReturnRecord)
			sss[0] = append(sss[0], "发起用户", "发起时间", "SQL语句", "运行状态", "会话时戳", "会话序号", "句柄序号")
			index = 1
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7)
				if err != nil {
					return nil, err
				}
				sss[index] = append(sss[index], string(s1), string(s2), string(s3), string(s4), string(s5), string(s6), string(s7))
				index++
			}
			sss = sss[0:index]
			return sss, nil
		case GetMasterServerList, GetTabletServerList, GetServerList:
			var s2, s3 string
			var s1, s4, s5, s6, s7, s8, s9, s10 []byte
			ss := make([]string, MaxResultReturnRecord)
			index = 0
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10)
				if err != nil {
					return nil, err
				}
				switch cmd {
				case GetMasterServerList:
					s2 = strings.ToLower(s2)
					s3 = strings.ToLower(s3)
					if strings.Index(s3, "standby") < 0 && strings.Index(s3, "offline") < 0 &&
						strings.Index(s2, "tablet") < 0 {
						ss[index] = string(s1)
						index++
					}
				case GetTabletServerList:
					s2 = strings.ToLower(s2)
					s3 = strings.ToLower(s3)
					if strings.Index(s3, "standby") < 0 && strings.Index(s3, "offline") < 0 &&
						strings.Index(s2, "master") < 0 {
						ss[index] = string(s1)
						index++
					}
				case GetServerList:
					s3 = strings.ToLower(s3)
					if strings.Index(s3, "standby") < 0 && strings.Index(s3, "offline") < 0 {
						ss[index] = string(s1)
						index++
					}
				}
			}
			return ss[0:index], nil
		case GetServerStatus:
			var s1, s2, s3, s4, s5, s6, s7, s8, s9, s10 []byte
			sss := make([][]string, MaxResultReturnRecord)
			sss[0] = append(sss[0], "服务器", "类型", "状态", "JAVA", "系统", "处理器", "核心", "内存", "磁盘", "系统时间")
			index = 1
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10)
				if err != nil {
					return nil, err
				}
				sss[index] = append(sss[index], string(s1), string(s2), string(s3), string(s4), string(s5),
					string(s6), string(s7), string(s8), string(s9), string(s10))
				index++
			}
			sss = sss[0:index]
			return sss, nil
		case GetSystemUtilization:
			var s1, s2, s3, s4, s5 []byte
			sss := make([][]string, MaxResultReturnRecord)
			sss[0] = append(sss[0], "服务器", "类型", "状态", "系统利用率", "进程利用率")
			index = 1
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2, &s3, &s4, &s5)
				if err != nil {
					return nil, err
				}
				sss[index] = append(sss[index], string(s1), string(s2), string(s3), string(s4), string(s5))
				index++
			}
			sss = sss[0:index]
			return sss, nil
		case GetMemorySize:
			var s1, s2, s3 []byte
			var s4, s5, s6, s7, s8 string
			var d4, d5, d6, d7, d8 float64
			sss := make([][]string, MaxResultReturnRecord)
			sss[0] = append(sss[0], "服务器", "类型", "状态", "物理内存", "配置内存", "提交内存", "已用内存", "交换内存")
			index = 1
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2, &s3, &d4, &d5, &d6, &d7, &d8)
				if err != nil {
					return nil, err
				}
				s4 = strconv.FormatFloat(d4/(1024*1024*1024), 'f', 2, 64)
				s5 = strconv.FormatFloat(d5/(1024*1024*1024), 'f', 2, 64)
				s6 = strconv.FormatFloat(d6/(1024*1024*1024), 'f', 2, 64)
				s7 = strconv.FormatFloat(d7/(1024*1024*1024), 'f', 2, 64)
				s8 = strconv.FormatFloat(d8/(1024*1024*1024), 'f', 2, 64)
				sss[index] = append(sss[index], string(s1), string(s2), string(s3), s4, s5, s6, s7, s8)
				index++
			}
			sss = sss[0:index]
			return sss, nil
		case GetNetworkStatus:
			var s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14 []byte
			sss := make([][]string, MaxResultReturnRecord)
			sss[0] = append(sss[0], "服务器", "类型", "网卡", "时戳", "接收字节", "接收包", "接收错误",
				"接收丢包", "接收速率", "发送字节", "发送包", "发送错误", "发送丢包", "发送速率")
			index = 1
			for index < MaxResultReturnRecord && rows.Next() {
				err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10, &s11, &s12, &s13, &s14)
				if err != nil {
					return nil, err
				}
				sss[index] = append(sss[index], string(s1), string(s2), string(s3), string(s4), string(s5),
					string(s6), string(s7), string(s8), string(s9), string(s10),
					string(s11), string(s12), string(s13), string(s14))
				index++
			}
			sss = sss[0:index]
			return sss, nil
		case GetUserNameList:
			var s1 string
			var s2 []byte
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

//	cmd :  GetTableNameList, GetViewNameList
func (db *DbWorker) GetNameList(cmd commandType, catalog string, schema []byte) ([]string, error) {
	cd := convert(cmd)
	var js []byte
	switch cmd {
	case GetTableNameList:
		js, _ = json.Marshal([]string{"TABLE"})
	case GetViewNameList:
		js, _ = json.Marshal([]string{"VIEW"})
	}
	rows, err := db.Db.Query("CloudWave", cd, schema, []byte(nil), json.RawMessage(js))
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4, s5, s6, s7, s8, s9, s10 []byte
	ss := make([]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10)
		if err != nil {
			return nil, err
		}
		ss[index] = string(s3)
		index++
	}
	ss = ss[0:index]
	if index > 1 {
		sort.Sort(sort.StringSlice(ss))
	}
	return ss, nil
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

func (db *DbWorker) GetTableComment(catalog string, schema []byte, table []byte) (string, error) {
	cd := convert(GetTableComment)
	var js []byte
	js, _ = json.Marshal([]string{"TABLE"})
	rows, err := db.Db.Query("CloudWave", cd, schema, table, json.RawMessage(js))
	defer rows.Close()
	if err != nil {
		return "", err
	}
	var s1, s2, s3, s4, s5, s6, s7, s8, s9, s10 []byte
	str := ""
	if rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10)
		if err != nil {
			return "", err
		}
		if s5 != nil {
			str = string(s5)
		} else {
			str = ""
		}
	}
	return str, nil
}

func getMoreData(result [][][]byte, count int, columns []int, makealiasArray bool) ([][]string, []string) {
	columnCount := len(result)
	baseIndex := 0
	if strings.Compare(string(result[0][0]), AUTOKEY_COLUMN) == 0 {
		baseIndex = 1
	}
	columnCount -= baseIndex
	if columnCount < 1 {
		return make([][]string, 0), make([]string, 0)
	}

	records := make([][]string, count)
	var aliasArray []string
	if makealiasArray {
		aliasArray = make([]string, count)
	}
	for i := 0; i < count; i++ {
		record := make([]string, len(columns))
		for j := 0; j < len(columns); j++ {
			if result[i][baseIndex+columns[j]] != nil {
				record[j] = string(result[i][baseIndex+columns[j]])
			} else {
				record[j] = "NULL"
			}
		}
		if makealiasArray {
			if result[i][baseIndex+22] != nil {
				aliasArray[i] = string(result[i][baseIndex+22])
			} else {
				aliasArray[i] = "NULL"
			}
		}
		records[i] = record
	}
	if makealiasArray {
		return records, aliasArray
	} else {
		return records, nil
	}
}

func (db *DbWorker) GetTableColumns(schema []byte, table []byte, requestID int64) ([][]string, error) {
	result, err := db.getTableColumns(schema, table)
	if err != nil {
		return nil, err
	}
	recordCount := len(result)

	pkResult, err := db.getPrimaryKeys("DEFAULT_", string(schema), string(table))
	if err != nil {
		return nil, err
	}
	count := len(pkResult)
	pkColumns := make([]string, count)
	pkSequences := make([]int64, count)
	var pkName []byte = nil
	for i := 0; i < count; i++ {
		if pkName == nil {
			pkName = pkResult[i][5]
		}
		pkColumns[i] = string(pkResult[i][3])
		pkSequence, _ := strconv.ParseInt(string(pkResult[i][4]), 10, 64)
		pkSequences[i] = int64(pkSequence)
	}

	ukResult, err := db.getUniqueKeys("DEFAULT_", string(schema), string(table))
	if err != nil {
		return nil, err
	}
	count = len(ukResult)
	ukColumns := make([]string, count)
	var ukName []byte = nil
	for i := 0; i < count; i++ {
		if ukName == nil {
			ukName = ukResult[i][5]
		}
		ukColumns[i] = string(ukResult[i][3])
	}

	ftiColumnsString, err := db.getFullTextIndexColumns("DEFAULT_", string(schema), string(table))
	if err != nil {
		return nil, err
	}
	tokens := strings.Split(ftiColumnsString, ",")
	ftiColumns := make([]string, len(tokens))
	j := 0
	for i := 0; i < len(tokens); i++ {
		if len(tokens[i]) > 0 {
			ftiColumns[j] = tokens[i]
			j++
		}
	}
	ftiColumns = ftiColumns[0:j]

	textIndexColumnsString, err := db.getTextIndexColumns("DEFAULT_", string(schema), string(table))
	if err != nil {
		return nil, err
	}
	tokens = strings.Split(textIndexColumnsString, ",")
	textIndexColumns := make([]string, len(tokens))
	j = 0
	for i := 0; i < len(tokens); i++ {
		if len(tokens[i]) > 0 {
			textIndexColumns[j] = tokens[i]
			j++
		}
	}
	textIndexColumns = textIndexColumns[0:j]

	indexColumnsString, err := db.getIndexColumns("DEFAULT_", string(schema), string(table))
	if err != nil {
		return nil, err
	}
	tokens = strings.Split(indexColumnsString, ",")
	indexColumns := make([]string, len(tokens))
	j = 0
	for i := 0; i < len(tokens); i++ {
		if len(tokens[i]) > 0 {
			indexColumns[j] = tokens[i]
			j++
		}
	}
	indexColumns = indexColumns[0:j]
	var columns = []int{3, 5, 6, 12, 17, 21, 11}
	rows, aliasArray := getMoreData(result, recordCount, columns, true)

	//	aliasArray := make([]string, recordCount)
	for i := 0; i < len(rows); i++ {
		if strings.Compare(string(rows[i][0]), AUTOKEY_COLUMN) == 0 {
			recordCount--
		}
	}
	array := make([][]string, recordCount+1)
	array[0] = append(array[0], "字段", "类型", "长度", "默认值", "可空", "自增", "索引", "文本", "全文", "唯一", "主键", "键序", "注释", "别名")
	record2 := make([]string, len(array[0]))
	j = 1
	for i := 0; i < len(rows); i++ {
		extendIndex := len(rows[i]) - 1
		remark := rows[i][extendIndex]
		if strings.Compare(rows[i][0], AUTOKEY_COLUMN) == 0 {
			continue
		}
		for j := 0; j <= extendIndex; j++ {
			record2[j] = rows[i][j]
		}
		ki := strings.Index(rows[i][1], "(")
		if ki > 0 {
			record2[1] = rows[i][1][0:ki]
			record2[2] = rows[i][1][ki+1 : len(rows[i][1])-1]
			if strings.EqualFold(record2[1], "NUMERIC") {
				record2[1] = "NUMBER"
			}
		}
		// replace NULL value
		if record2[3] == "NULL" || record2[3] == "" {
			record2[3] = "-"
		}

		k := 0
		for k < len(indexColumns) {
			if indexColumns[k] == record2[0] {
				break
			}
			k++
		}
		if k < len(indexColumns) {
			record2[extendIndex] = "YES"
		} else {
			record2[extendIndex] = "NO"
		}

		k = 0
		for k < len(textIndexColumns) {
			if textIndexColumns[k] == record2[0] {
				break
			}
			k++
		}
		if k < len(textIndexColumns) {
			record2[extendIndex+1] = "YES"
		} else {
			record2[extendIndex+1] = "NO"
		}

		k = 0
		for k < len(ftiColumns) {
			if ftiColumns[k] == record2[0] {
				break
			}
			k++
		}
		if k < len(ftiColumns) {
			record2[extendIndex+2] = "YES"
		} else {
			record2[extendIndex+2] = "NO"
		}

		k = 0
		for k < len(ukColumns) {
			if ukColumns[k] == record2[0] {
				break
			}
			k++
		}
		if k < len(ukColumns) {
			record2[extendIndex+3] = "YES"
		} else {
			record2[extendIndex+3] = "NO"
		}

		k = 0
		for k < len(pkColumns) {
			if pkColumns[k] == record2[0] {
				break
			}
			k++
		}
		if k < len(pkColumns) {
			record2[extendIndex+4] = "YES"
			record2[extendIndex+5] = strconv.FormatInt(pkSequences[k], 10)
		} else {
			record2[extendIndex+4] = "NO"
			record2[extendIndex+5] = "-"
		}

		if remark != "NULL" && remark != "" {
			record2[extendIndex+6] = remark
		} else {
			record2[extendIndex+6] = "-"
		}
		if aliasArray[i] != "NULL" && aliasArray[i] != "" {
			record2[extendIndex+7] = aliasArray[i]
		} else {
			record2[extendIndex+7] = "-"
		}
		for k := 0; k < len(record2); k++ {
			array[j] = append(array[j], record2[k])
		}
		j++
	}
	return array, nil
}

func (db *DbWorker) GetTableDefinition(schema []byte, table []byte, requestID int64) (string, error) {
	columns, err := db.GetTableColumns(schema, table, requestID)
	if err != nil {
		return "", err
	}
	columnsCount := len(columns)
	pkColumns := make([]string, columnsCount)
	pkSequences := make([]int, columnsCount)
	indexColumns := make([]string, columnsCount)
	fulltextColumns := make([]string, columnsCount)
	textColumns := make([]string, columnsCount)
	ukColumns := make([]string, columnsCount)
	columnRemarks := make([]string, columnsCount)
	pkColumnsCount := 0
	pkSequencesCount := 0
	indexColumnsCount := 0
	fulltextColumnsCount := 0
	textColumnsCount := 0
	ukColumnsCount := 0
	columnRemarksCount := 0
	var sb strings.Builder
	sb.WriteString("CREATE TABLE " + string(schema) + "." + string(table) + "(\n")
	for i := 1; i < columnsCount; i++ {
		sb.WriteString("  ")
		sb.WriteString(columns[i][0] + " ")
		if strings.Compare(columns[i][1], "VARCHAR") == 0 || strings.Compare(columns[i][1], "CHAR") == 0 ||
			strings.Compare(columns[i][1], "VARBINARY") == 0 || strings.Compare(columns[i][1], "BINARY") == 0 ||
			strings.Compare(columns[i][1], "NUMBER") == 0 || strings.Compare(columns[i][1], "NUMERIC") == 0 {
			sb.WriteString(columns[i][1] + "(" + columns[i][2] + ") ")
		} else if strings.Compare(columns[i][5], "YES") == 0 {
			sb.WriteString("AUTO_INCREMENT ")
		} else {
			sb.WriteString(columns[i][1] + " ")
		}
		if strings.Compare(columns[i][3], "NULL") != 0 && strings.Compare(columns[i][3], "-") != 0 {
			sb.WriteString("DEFAULT '" + columns[i][3] + "' ")
		}

		if strings.Compare(columns[i][6], "YES") == 0 {
			indexColumns[indexColumnsCount] = columns[i][0]
			indexColumnsCount++
		}
		if strings.Compare(columns[i][7], "YES") == 0 {
			textColumns[textColumnsCount] = columns[i][0]
			textColumnsCount++
		}
		if strings.Compare(columns[i][8], "YES") == 0 {
			fulltextColumns[fulltextColumnsCount] = columns[i][0]
			fulltextColumnsCount++
		}
		if strings.Compare(columns[i][9], "YES") == 0 {
			ukColumns[ukColumnsCount] = columns[i][0]
			ukColumnsCount++
		}
		if strings.Compare(columns[i][10], "YES") == 0 {
			pkColumns[pkColumnsCount] = columns[i][0]
			pkColumnsCount++
			n, _ := strconv.Atoi(columns[i][11])
			pkSequences[pkSequencesCount] = n
			pkSequencesCount++
		} else {
			if strings.Compare(columns[i][4], "NO") == 0 {
				sb.WriteString("NOT NULL ")
			}
		}
		if strings.Compare(columns[i][12], "-") != 0 {
			columnRemarks[columnRemarksCount] = "COMMENT ON COLUMN " + string(schema) + "." + string(table) + "." + columns[i][0] + " is '" + columns[i][12] + "';\n"
		}
		if i < columnsCount-1 || pkColumnsCount > 0 || ukColumnsCount > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("\n")
	}
	if pkColumnsCount > 0 {
		sb.WriteString("  PRIMARY KEY (")
		for j := 0; j < pkColumnsCount; j++ {
			pki := pkSequences[j]
			sb.WriteString(pkColumns[pki])
			if j < pkColumnsCount-1 {
				sb.WriteString(", ")
			} else {
				sb.WriteString(")\n")
			}
		}
	} else if ukColumnsCount > 0 {
		sb.WriteString("  UNIQUE (")
		for j := 0; j < ukColumnsCount; j++ {
			sb.WriteString(string(ukColumns[j]))
			if j < ukColumnsCount-1 {
				sb.WriteString(", ")
			} else {
				sb.WriteString(")\n")
			}
		}
	}
	sb.WriteString(");\n")

	if indexColumnsCount > 0 {
		sb.WriteString("\n")
		for j := 0; j < indexColumnsCount; j++ {
			sb.WriteString("CREATE INDEX ON " + string(schema) + "." + string(table) + "(" + indexColumns[j] + ");\n")
		}
	}
	if textColumnsCount > 0 {
		sb.WriteString("\n")
		for j := 0; j < textColumnsCount; j++ {
			sb.WriteString("CREATE TEXT INDEX ON " + string(schema) + "." + string(table) + "(" + textColumns[j] + ");\n")
		}
	}
	if fulltextColumnsCount > 0 {
		sb.WriteString("CREATE FULLTEXT INDEX ON " + string(schema) + "." + string(table) + "(")
		for j := 0; j < fulltextColumnsCount; j++ {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fulltextColumns[j])
		}
		sb.WriteString(");\n")
	}
	sb.WriteString("\n")

	tableRemark, _ := db.GetTableComment("", schema, table)
	if tableRemark != "" {
		sb.WriteString("COMMENT ON TABLE " + string(schema) + "." + string(table) + " is '" + tableRemark + "';\n")
	}
	for j := 0; j < columnRemarksCount; j++ {
		sb.WriteString(columnRemarks[j])
	}
	return sb.String(), nil
}

func (db *DbWorker) GetViewDefinition(catalog string, schema []byte, table []byte) ([][]string, error) {
	cd := convert(GetViewDefinition)
	js, _ := json.Marshal([]string{"VIEW"})
	rows, err := db.Db.Query("CloudWave", cd, schema, table, json.RawMessage(js))
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4, s5, s6, s7, s8, s9, s10 []byte
	sss := make([][][]byte, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10)
		if err != nil {
			return nil, err
		}
		sss[index] = append(sss[index], s1, s2, s3, s4, s5, s6, s7, s8, s9, s10)
		index++
		if index >= MaxResultReturnRecord {
			break
		}
	}
	sss = sss[0:index]
	var columns = []int{2, 4}
	records, _ := getMoreData(sss, index, columns, false)
	array := make([][]string, index+1)
	array[0] = append(array[0], "VIEW", "DESCRIPTION")
	j := 1
	for i := 0; i < index; i++ {
		for k := 0; k < len(records[i]); k++ {
			array[j] = append(array[j], records[i][k])
		}
		j++
	}
	return array, nil
}

func (db *DbWorker) GetTabletData(schema string, table string, tabletID int64, requestID int64) ([][]string, error) {
	cd := convert(GetTabletData)
	rows, err := db.Db.Query("CloudWave", cd, schema, table, tabletID)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14 []byte
	sss := make([][]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10, &s11, &s12, &s13, &s14)
		if err != nil {
			return nil, err
		}
		sss[index] = append(sss[index], string(s1), string(s2), string(s3), string(s4), string(s5),
			string(s6), string(s7), string(s8), string(s9), string(s10),
			string(s11), string(s12), string(s13), string(s14))
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
	var s1, s2, s3, s4 []byte
	sss := make([][]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4)
		if err != nil {
			return nil, err
		}
		sss[index] = append(sss[index], string(s1), string(s2), string(s3), string(s4))
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
	var s1, s2, s3, s4 []byte
	sss := make([][]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4)
		if err != nil {
			return nil, err
		}
		sss[index] = append(sss[index], string(s1), string(s2), string(s3), string(s4))
		index++
		if index >= MaxResultReturnRecord {
			break
		}
	}
	sss = sss[0:index]
	/*
		tserverTabletCountMap := make(map[string] int64)
		tserverSumRecordCountMap := make(map[string] int64)
		tserverMaxRecordCountMap := make(map[string] int64)
		tserverTabletScattersList := make(map[string] []string)
		for i := 0; i < index; i++ {
			var tabletId, tabletServer string
			var tabletNo, tabletRcount int64
			ki := strings.Index(sss[i][0], ",")
			tabletId = sss[i][0]
			if ki > 0 {
				tabletId = sss[i][0][0:ki]
			}
			tabletNo, _ = strconv.ParseInt(sss[i][0][ki + 1:len(sss[i][0])], 10, 64)
			tabletRcount, _ = strconv.ParseInt(sss[i][2], 10, 64)
			tabletServer = sss[i][3]
			var tserverTabletCount int64
			tserverTabletCount = 0
			if v, ok := tserverTabletCountMap[tabletServer]; ok {
				tserverTabletCount = v
			}
			tserverTabletCount++
			tserverTabletCountMap[tabletServer] = tserverTabletCount

			var tserverSumRecordCount int64
			tserverSumRecordCount = 0
			if v, ok := tserverSumRecordCountMap[tabletServer]; ok {
				tserverSumRecordCount = v
			}
			tserverSumRecordCount += tabletRcount
			tserverSumRecordCountMap[tabletServer] = tserverSumRecordCount

			var tserverMaxRecordCount int64
			tserverMaxRecordCount = 0
			if v, ok := tserverMaxRecordCountMap[tabletServer]; ok {
				tserverMaxRecordCount = v
			}
			if tserverMaxRecordCount < tabletRcount {
				tserverMaxRecordCount = tabletRcount
			}
			tserverMaxRecordCountMap[tabletServer] = tserverMaxRecordCount

			var scatters []string
			if i < 1000 {
				if v, ok := tserverTabletScattersList[tabletServer]; ok {
					scatters = v
				}
				ArrayList<String[]> scatters = tserverTabletScattersList.get(tabletServer);
				if (scatters == null) {
					scatters = new ArrayList<>();
				}
				String[] scatter = new String[] {
					String.valueOf(tabletNo),
					String.valueOf(tabletRcount)
				};
				scatters.add(scatter);
				tserverTabletScattersList.put(tabletServer, scatters);
			}
		}

		String[][] tserverDistribs = new String[tabletServers.size()][];
		int i = 0
		for i := 0; i < index; i++ {
			tserver := sss[i][3]

			ArrayList<String> values = new ArrayList<>();
			values.add(tserver);

			Long tabletCount = tserverTabletCountMap.get(tserver);
			if (tabletCount == null) {
				values.add(String.valueOf(0));
			} else {
				values.add(String.valueOf(tabletCount));
			}

			Long tabletSumRecordCount = tserverSumRecordCountMap.get(tserver);
			if (tabletSumRecordCount == null) {
				values.add(String.valueOf(0));
			} else {
				values.add(String.valueOf(tabletSumRecordCount / tabletCount));
			}

			Long tabletMaxRecordCount = tserverMaxRecordCountMap.get(tserver);
			if (tabletMaxRecordCount == null) {
				values.add(String.valueOf(0));
			} else {
				values.add(String.valueOf(tabletMaxRecordCount));
			}

			ArrayList<String[]> scatters = tserverTabletScattersList.get(tserver);
			if (scatters != null) {
				for (String[] scatter : scatters) {
					values.add(scatter[0]);
					values.add(scatter[1]);
				}
			}
			tserverDistribs[i ++] = values.toArray(new String[0]);
		}

		return tserverDistribs;
	*/
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
	return str, err
}

func (db *DbWorker) SaveConfigOptions(options []string) ([]string, error) {
	cd := convert(SaveConfigOptions)
	n := 4
	for i := 0; i < len(options); i++ {
		n += 4 + len(options[i])
	}
	buffer := make([]byte, n)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(len(options)))
	n = 4
	for i := 0; i < len(options); i++ {
		binary.BigEndian.PutUint32(buffer[n:n+4], uint32(len(options[i])))
		n += 4
		n += copy(buffer[n:], options[i])
	}

	resExec, err := db.Db.Exec("CloudWave", cd, buffer)
	if err != nil {
		return nil, err
	}
	i, _ := resExec.RowsAffected()
	buf := cloudwave.PullData(int(i))
	if len(buf) < 5 {
		return nil, errors.New("result is null")
	}
	count := int(binary.BigEndian.Uint32(buf[1:]))
	pos := 5
	ss := make([]string, count)
	var s string
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
	rows, err := db.Db.Query("CloudWave", cd, nil)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2 []byte
	ss := make([]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2)
		if err != nil {
			return nil, err
		}
		ss[index] = string(s1)
		index++
		if index >= MaxResultReturnRecord {
			break
		}
	}
	ss = ss[0:index]
	if index > 1 {
		sort.Sort(sort.StringSlice(ss))
	}
	return ss, nil
}

func (db *DbWorker) GetSchemas(catalog string, schemaPattern []byte) ([]string, error) {
	cd := convert(GetSchemas)
	rows, err := db.Db.Query("CloudWave", cd, nil)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2 []byte
	ss := make([]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2)
		if err != nil {
			return nil, err
		}
		ss[index] = string(s1)
		index++
		if index >= MaxResultReturnRecord {
			break
		}
	}
	ss = ss[0:index]
	return ss, nil
}

func (db *DbWorker) GetTables(catalog string, schemaPattern []byte, tableNamePattern []byte) ([]string, error) {
	cd := convert(GetTables)
	var js []byte
	js, _ = json.Marshal([]string{"TABLE"})
	rows, err := db.Db.Query("CloudWave", cd, schemaPattern, tableNamePattern, json.RawMessage(js))
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4, s5, s6, s7, s8, s9, s10 []byte
	ss := make([]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10)
		if err != nil {
			return nil, err
		}
		ss[index] = string(s3)
		index++
		if index >= MaxResultReturnRecord {
			break
		}
	}
	return ss[0:index], nil
}

func (db *DbWorker) GetViews(catalog string, schemaPattern []byte, tableNamePattern []byte) ([]string, error) {
	cd := convert(GetViews)
	var js []byte
	js, _ = json.Marshal([]string{"VIEW"})
	rows, err := db.Db.Query("CloudWave", cd, schemaPattern, tableNamePattern, json.RawMessage(js))
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4, s5, s6, s7, s8, s9, s10 []byte
	ss := make([]string, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10)
		if err != nil {
			return nil, err
		}
		ss[index] = string(s3)
		index++
		if index >= MaxResultReturnRecord {
			break
		}
	}
	return ss[0:index], nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (db *DbWorker) getTableColumns(schema []byte, table []byte) ([][][]byte, error) {
	cd := convert(GetTableColumns)
	rows, err := db.Db.Query("CloudWave", cd, schema, table, []byte(nil))
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22, s23, s24, s25 []byte
	sss := make([][][]byte, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9, &s10, &s11, &s12,
			&s13, &s14, &s15, &s16, &s17, &s18, &s19, &s20, &s21, &s22, &s23, &s24, &s25)
		if err != nil {
			return nil, err
		}
		sss[index] = append(sss[index], s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,
			s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22, s23, s24, s25)
		index++
		if index >= MaxResultReturnRecord {
			break
		}
	}
	return sss[0:index], nil
}
func (db *DbWorker) getPrimaryKeys(catalog string, schema string, table string) ([][][]byte, error) {
	cd := convert(GetPrimaryKeys)
	rows, err := db.Db.Query("CloudWave", cd, schema, table)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4, s5, s6 []byte
	sss := make([][][]byte, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6)
		if err != nil {
			return nil, err
		}
		sss[index] = append(sss[index], s1, s2, s3, s4, s5, s6)
		index++
	}
	return sss[0:index], nil
}

func (db *DbWorker) getUniqueKeys(catalog string, schema string, table string) ([][][]byte, error) {
	cd := convert(GetUniqueKeys)
	rows, err := db.Db.Query("CloudWave", cd, schema, table)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	var s1, s2, s3, s4, s5, s6 []byte
	sss := make([][][]byte, MaxResultReturnRecord)
	index := 0
	for index < MaxResultReturnRecord && rows.Next() {
		err = rows.Scan(&s1, &s2, &s3, &s4, &s5, &s6)
		if err != nil {
			return nil, err
		}
		sss[index] = append(sss[index], s1, s2, s3, s4, s5, s6)
		index++
	}
	return sss[0:index], nil
}

func (db *DbWorker) getFullTextIndexColumns(catalog string, schema string, table string) (string, error) {
	cd := convert(GetFullTextIndexColumns)
	resExec, err := db.Db.Exec("CloudWave", cd, schema, table)
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

func (db *DbWorker) getTextIndexColumns(catalog string, schema string, table string) (string, error) {
	cd := convert(GetTextIndexColumns)
	resExec, err := db.Db.Exec("CloudWave", cd, schema, table)
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

func (db *DbWorker) getIndexColumns(catalog string, schema string, table string) (string, error) {
	cd := convert(GetIndexColumns)
	resExec, err := db.Db.Exec("CloudWave", cd, schema, table)
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
