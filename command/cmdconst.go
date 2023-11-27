package command


const  MaxResultReturnRecord = 1000

type commandType int

const (
	GetResultTaskStatistics commandType = iota // 137 Exec * 无返回数据
	GetTableComment         // 17 Query * 出错
	GetTableColumns         // 20 Query * Scan 时出错（第7列数据为NULL）
	GetTableDefinition      // 20 use getTableColumns
	GetViewDefinition       // ??? 不知如何执行
	GetTabletData           // 75 Query * 无返回数据
	GetSchemaBfiles         // 111 get user executeQuery(sql, 搭"select * from bfiles"执行
	GetTableDistribution    // 71 Query *****
	GetTableDistributionStatistics  // 71 Query * 需要处理返回的数据
	GetOnlineSessions       // 82 Exec * no param 返回的数据缺一列
	GetRunningSQL           // 80 Query * no param 无返回数据
	GetSQLStatistics        // 135 Exec *****
	GetHistorySQLs          // 128 Exec * 无返回数据
	GetCloudwaveVersion     // 103 Exec ***** no param
	GetUserPrivileges       // 114 Exec * Unsupported request type code: 114
	GetMasterServerList     // 70 Query *** no param
	GetTabletServerList     // 70 Query *** no param
	GetServerList           // 70 Query *** no param
	GetServerStatus         // 70 Query *** no param
	GetSystemUtilization    // 106 Query *** no param
	GetMemorySize           // 107 Query *** no param
	GetNetworkStatus        // 132 Query ***** no param
	GetDfsStatus            // 76 Exec *** no param
	GetConfigOptions        // 108 Exec *** no param
	SaveConfigOptions       // 131 ??? 写数据到服务器
	GetServerLogger         // 140 Exec ***
	GetProcessJstack        // 93 Exec ***
	GetHealthDiagnostic     // 133 Exec * Unsupported request type code: 133
	GetSystemOverview       // 109 Exec * no param  Unsupported request type code: 109
	DoRestartServer         // 113 Exrc * 发完命令后还需再连接
	GetSchemaNameList       // 15 Query *****
	GetTableNameList        // 17 Query * 出错
	GetViewNameList         // 17 Query * 出错
	GetUserNameList         // 25 Query ***** no param
	GetSchemas              // ??? 不知如何执行
	GetTables               // ??? 不知如何执行
	GetViews                // ??? 不知如何执行
	GetRuntimeReport        // 130 Exec ***** no param

//	GetCatalogs             // 24
//	GetUsers                // 25
//	GetTabletIds            // 96
)

