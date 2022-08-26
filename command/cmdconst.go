package command


const  MaxResultReturnRecord = 1000

type commandType int

const (
	GetResultTaskStatistics commandType = iota // 137 Exec *** 无返回数据
	GetTableComment         // 17 Query *****
	GetTableColumns         // 20 Query *****
	GetTableDefinition      // 20 **** use getTableColumns 返回为string，而不是[][]string
	GetViewDefinition       // 17 Query *****
	GetTabletData           // 75 Query * 无返回数据
	GetSchemaBfiles         // 111 get user executeQuery(sql, 搭"select * from bfiles"执行
	GetTableDistribution    // 71 Query *****
	GetTableDistributionStatistics  // 71 Query * 需要处理返回的数据
	GetOnlineSessions       // 82 Exec ***** no param
	GetRunningSQL           // 80 Query * no param 无返回数据
	GetSQLStatistics        // 135 Exec *****
	GetHistorySQLs          // 128 Exec *****
	GetCloudwaveVersion     // 103 Exec ***** no param
	GetUserPrivileges       // 114 Exec * Unsupported request type code: 114     出错
	GetMasterServerList     // 70 Query ***** no param
	GetTabletServerList     // 70 Query ***** no param
	GetServerList           // 70 Query ***** no param
	GetServerStatus         // 70 Query ***** no param
	GetSystemUtilization    // 106 Query ***** no param
	GetMemorySize           // 107 Query ***** no param
	GetNetworkStatus        // 132 Query ***** no param
	GetDfsStatus            // 76 Exec ***** no param
	GetConfigOptions        // 108 Exec ***** no param
	SaveConfigOptions       // 131 Exec ***** 写数据到服务器  需要了解写入服务器的数据格式
	GetServerLogger         // 140 Exec *****
	GetProcessJstack        // 93 Exec *****
	GetHealthDiagnostic     // 133 Exec * 需要处理返回的数据
	GetSystemOverview       // 109 Exec *****
	DoRestartServer         // 113 Exrc * 发完命令后还需再连接
	GetSchemaNameList       // 15 Query *****
	GetTableNameList        // 17 Query *****
	GetViewNameList         // 17 Query ***** 为什么与GetTableNameList返回一样
	GetUserNameList         // 25 Query ***** no param
	GetSchemas              // 15 Query *****
	GetTables               // 17 Query *****
	GetViews                // 17 Query *****
	GetRuntimeReport        // 130 Exec ***** no param

	//以下5个是被其他方法调用的，大部分没有返回数据
	GetPrimaryKeys          // 21 Query
	GetUniqueKeys           // 124 Query
	GetFullTextIndexColumns // 94 Exec
	GetTextIndexColumns     // 123 Exec
	GetIndexColumns         // 115 Exec

//	GetCatalogs             // 24
//	GetUsers                // 25
//	GetTabletIds            // 96
)

const  AUTOKEY_COLUMN = "__CLOUDWAVE_AUTO_KEY__"


