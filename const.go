// Go CloudWave Driver - A CloudWave-Driver for Go's database/sql package
//
// Copyright 2012 The Go-CloudWave-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package cloudwave

const (
	defaultAuthPlugin       = "mysql_native_password"
	defaultMaxAllowedPacket = 4 << 20 // 4 MiB
	minProtocolVersion      = 10
	maxPacketSize           = 1<<24 - 1
	dateFormat              = "2006-01-02"
	timeFormat              = "2006-01-02 15:04:05.00000"

	INT_MIN_VALUE = 0x80000000
	INT_MAX_VALUE = 0x7fffffff

	LONG_MIN_VALUE = 0x8000000000000000
	LONG_MAX_VALUE = 0x7fffffffffffffff

	INT_CHUNK_SIZE = 8192
)

// MySQL constants documentation:
// http://dev.cloudwave.com/doc/internals/en/client-server-protocol.html

const (
	iERR byte = 0x00
	iOK  byte = 0x01
	iEOF byte = 0xff // Can not be used
)

// https://dev.cloudwave.com/doc/internals/en/capability-flags.html#packet-Protocol::CapabilityFlags
type clientFlag uint32

const (
	clientLongPassword clientFlag = 1 << iota
	clientFoundRows
	clientLongFlag
	clientConnectWithDB
	clientNoSchema
	clientCompress
	clientODBC
	clientLocalFiles
	clientIgnoreSpace
	clientProtocol41
	clientInteractive
	clientSSL
	clientIgnoreSIGPIPE
	clientTransactions
	clientReserved
	clientSecureConn
	clientMultiStatements
	clientMultiResults
	clientPSMultiResults
	clientPluginAuth
	clientConnectAttrs
	clientPluginAuthLenEncClientData
	clientCanHandleExpiredPasswords
	clientSessionTrack
	clientDeprecateEOF
)

/*
const (
	comQuit byte = iota + 1
	comInitDB
	comQuery
	comFieldList
	comCreateDB
	comDropDB
	comRefresh
	comShutdown
	comStatistics
	comProcessInfo
	comConnect
	comProcessKill
	comDebug
	comPing
	comTime
	comDelayedInsert
	comChangeUser
	comBinlogDump
	comTableDump
	comConnectOut
	comRegisterSlave
	comStmtPrepare
	comStmtExecute
	comStmtSendLongData
	comStmtClose
	comStmtReset
	comSetOption
	comStmtFetch
)
*/

type columnHeaderFieldType int32

const (
	BIT = -7

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code TINYINT}.
	 */
	TINYINT = -6

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code SMALLINT}.
	 */
	SMALLINT = 5

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code INTEGER}.
	 */
	INTEGER = 4

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code BIGINT}.
	 */
	BIGINT = -5

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code FLOAT}.
	 */
	FLOAT = 6

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code REAL}.
	 */
	REAL = 7

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code DOUBLE}.
	 */
	DOUBLE = 8

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code NUMERIC}.
	 */
	NUMERIC = 2

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code DECIMAL}.
	 */
	DECIMAL = 3

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code CHAR}.
	 */
	CHAR = 1

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code VARCHAR}.
	 */
	VARCHAR = 12

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code LONGVARCHAR}.
	 */
	LONGVARCHAR = -1

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code DATE}.
	 */
	DATE = 91

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code TIME}.
	 */
	TIME = 92

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code TIMESTAMP}.
	 */
	TIMESTAMP = 93

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code BINARY}.
	 */
	BINARY = -2

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code VARBINARY}.
	 */
	VARBINARY = -3

	/**
	 * <P>The constant in the Java programming language, sometimes referred
	 * to as a type code, that identifies the generic SQL type
	 * {@code LONGVARBINARY}.
	 */
	LONGVARBINARY = -4

	/**
	 * <P>The constant in the Java programming language
	 * that identifies the generic SQL value
	 * {@code NULL}.
	 */
	NULL = 0

	/**
	 * The constant in the Java programming language that indicates
	 * that the SQL type is database-specific and
	 * gets mapped to a Java object that can be accessed via
	 * the methods {@code getObject} and {@code setObject}.
	 */
	OTHER = 1111

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type
	 * {@code JAVA_OBJECT}.
	 * @since 1.2
	 */
	JAVA_OBJECT = 2000

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type
	 * {@code DISTINCT}.
	 * @since 1.2
	 */
	DISTINCT = 2001

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type
	 * {@code STRUCT}.
	 * @since 1.2
	 */
	STRUCT = 2002

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type
	 * {@code ARRAY}.
	 * @since 1.2
	 */
	ARRAY = 2003

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type
	 * {@code BLOB}.
	 * @since 1.2
	 */
	BLOB = 2004

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type
	 * {@code CLOB}.
	 * @since 1.2
	 */
	CLOB = 2005

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type
	 * {@code REF}.
	 * @since 1.2
	 */
	REF = 2006

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type {@code DATALINK}.
	 *
	 * @since 1.4
	 */
	DATALINK = 70

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type {@code BOOLEAN}.
	 *
	 * @since 1.4
	 */
	BOOLEAN = 16

	//------------------------- JDBC 4.0 -----------------------------------

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type {@code ROWID}
	 *
	 * @since 1.6
	 *
	 */
	ROWID = -8

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type {@code NCHAR}
	 *
	 * @since 1.6
	 */
	NCHAR = -15

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type {@code NVARCHAR}.
	 *
	 * @since 1.6
	 */
	NVARCHAR = -9

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type {@code LONGNVARCHAR}.
	 *
	 * @since 1.6
	 */
	LONGNVARCHAR = -16

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type {@code NCLOB}.
	 *
	 * @since 1.6
	 */
	NCLOB = 2011

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type {@code XML}.
	 *
	 * @since 1.6
	 */
	SQLXML = 2009

	//--------------------------JDBC 4.2 -----------------------------

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type {@code REF CURSOR}.
	 *
	 * @since 1.8
	 */
	REF_CURSOR = 2012

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type
	 * {@code TIME WITH TIMEZONE}.
	 *
	 * @since 1.8
	 */
	TIME_WITH_TIMEZONE = 2013

	/**
	 * The constant in the Java programming language, sometimes referred to
	 * as a type code, that identifies the generic SQL type
	 * {@code TIMESTAMP WITH TIMEZONE}.
	 *
	 * @since 1.8
	 */
	TIMESTAMP_WITH_TIMEZONE = 2014
)

// https://dev.cloudwave.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
type fieldType byte

const (
	//ODBC
	CLOUD_TYPE_INTEGER             = 0
	CLOUD_TYPE_CHAR                = 1
	CLOUD_TYPE_VARCHAR             = 2
	CLOUD_TYPE_FLOAT               = 3
	CLOUD_TYPE_DOUBLE              = 4
	CLOUD_TYPE_DATE                = 5
	CLOUD_TYPE_TIME                = 6
	CLOUD_TYPE_TIMESTAMP           = 7
	CLOUD_TYPE_BOOLEAN             = 8
	CLOUD_TYPE_ARRAY               = 9
	CLOUD_TYPE_TINY_DECIMAL        = 10
	CLOUD_TYPE_SMALL_DECIMAL       = 11
	CLOUD_TYPE_BIG_DECIMAL         = 12
	CLOUD_TYPE_SINGLE_CHAR         = 13
	CLOUD_TYPE_BINARY              = 14
	CLOUD_TYPE_VARBINARY           = 15
	CLOUD_TYPE_SINGLE_BYTE         = 16
	CLOUD_TYPE_LONG                = 17
	CLOUD_TYPE_INTERVAL            = 18
	CLOUD_TYPE_REVERSE             = 19
	CLOUD_TYPE_BLOB                = 20
	CLOUD_TYPE_CLOB                = 21
	CLOUD_TYPE_LONGVARBINARY       = 22
	CLOUD_TYPE_LONGVARCHAR         = 23
	CLOUD_TYPE_ROWID               = 24
	CLOUD_TYPE_BIG_INTEGER         = 25
	CLOUD_TYPE_SMALL_INTEGER       = 26
	CLOUD_TYPE_TINY_INTEGER        = 27
	CLOUD_TYPE_DAY_TIME_INTERVAL   = 28
	CLOUD_TYPE_YEAR_MONTH_INTERVAL = 29
	CLOUD_TYPE_TIME_INTERVAL       = 30
	CLOUD_TYPE_PAIR                = 31
	CLOUD_TYPE_BYTE                = 32
	CLOUD_TYPE_BFILE               = 33

	//3.6.6 JDBC
	CLOUD_TYPE_INTS                = 34
	CLOUD_TYPE_JAVA_STRING         = 35
	CLOUD_TYPE_X1_LONG             = 36
	CLOUD_TYPE_X2_LONG             = 37
	CLOUD_TYPE_X_LONG              = 38
	CLOUD_TYPE_X_BYTES             = 39
	CLOUD_TYPE_RANGE_STRING        = 40
	CLOUD_TYPE_DEFAULT             = 41
	CLOUD_TYPE_RANK                = 42
	CLOUD_TYPE_COLLECTOR_SELECTION = 43
	CLOUD_TYPE_GROUPING            = 44
	CLOUD_TYPE_NULL                = 45
	CLOUD_TYPE_NOTHING             = 46
	CLOUD_TYPE_MEDIA               = 47
	CLOUD_TYPE_FISCAL_YEAR         = 48
	CLOUD_TYPE_FISCAL_QUARTER      = 49
	CLOUD_TYPE_YEAR_MONTH          = 50
	CLOUD_TYPE_YEAR_MONTH_DAY      = 51
	CLOUD_TYPE_NUMBER              = 52

	//2021.9.12 HU give wei JDBC
	CLOUD_TYPE_DOUBLE_LONG        = 53
	CLOUD_TYPE_COMPACT_DATE       = 54
	CLOUD_TYPE_SHARED_COLUMN      = 55
	CLOUD_TYPE_ZONE_AUTO_SEQUENCE = 56
	CLOUD_TYPE_JSON_ARRAY         = 57
	CLOUD_TYPE_JSON_KEYWORD       = 58
	CLOUD_TYPE_JSON_BINARY        = 59
	CLOUD_TYPE_JSON_TEXT          = 60
	CLOUD_TYPE_JSON_OBJECT        = 61
	CLOUD_TYPE_JSON_BIGDECIMAL    = 62

	CLOUD_TYPE_OTHER = -1
)

type fieldFlag uint16

const (
	flagNotNULL fieldFlag = 1 << iota
	flagPriKey
	flagUniqueKey
	flagMultipleKey
	flagBLOB
	flagUnsigned
	flagZeroFill
	flagBinary
	flagEnum
	flagAutoIncrement
	flagTimestamp
	flagSet
	flagUnknown1
	flagUnknown2
	flagUnknown3
	flagUnknown4
)

// http://dev.cloudwave.com/doc/internals/en/status-flags.html
type statusFlag uint16

const (
	statusInTrans statusFlag = 1 << iota
	statusInAutocommit
	statusReserved // Not in documentation
	statusMoreResultsExists
	statusNoGoodIndexUsed
	statusNoIndexUsed
	statusCursorExists
	statusLastRowSent
	statusDbDropped
	statusNoBackslashEscapes
	statusMetadataChanged
	statusQueryWasSlow
	statusPsOutParams
	statusInTransReadonly
	statusSessionStateChanged
)

const (
	cachingSha2PasswordRequestPublicKey          = 2
	cachingSha2PasswordFastAuthSuccess           = 3
	cachingSha2PasswordPerformFullAuthentication = 4
)

// for cloudwave
const (
	CLOUDWAVE_EXECUTE        = 0
	CLOUDWAVE_EXECUTE_UPDATE = 1
	CLOUDWAVE_EXECUTE_QUERY  = 2
	CLOUDWAVE_SELFUSEDRIVE   = 0xff
)

// for cloudwave
const (
	B_REQ_TAG                             byte = 0x02
	E_REQ_TAG                             byte = 0x03
	B_REQ_BUILD_CONNECTION                     = -1
	B_REQ_CLOSE_CONNECTION                     = -2
	B_REQ_PING                                 = -3
	B_REQ_STOP_SERVER                          = -4
	B_REQ_BFILE_WRITE                          = -5
	B_REQ_BFILE_READ                           = -6
	B_REQ_BFILE_CREATE                         = -7
	B_REQ_BFILE_SYNC                           = -8
	B_REQ_BFILE_GETBYNAME                      = -9
	B_REQ_BFILE_GETALL                         = -10
	B_REQ_BFILE_GET_SEGMENT_TABLET_SERVER      = -11
	B_REQ_BFILE_DELETE                         = -12
	B_REQ_TABLET_SERVER_CONNECT                = -13
	B_REQ_BFILE_CLOSE                          = -14
	B_REQ_BFILE_GET_BY_ID                      = -15
	B_REQ_BFILE_BATCH_WRITE                    = -16
	B_REQ_BFILE_BATCH_CREATE                   = -17
	B_REQ_BFILE_NFSBFILE_CREATE                = -18
	B_REQ_BFILE_GET_NFSBFILE_INPUTSTREAM       = -19
	B_REQ_GET_NEXT_TABLET_SERVER               = -31
	B_REQ_AUTO_TABLET_INSERT_FILES             = -32
	B_REQ_AUTO_TABLET_SYNC                     = -33

	CONNECTION_SET_AUTO_COMMIT                   = 1
	CONNECTION_CREATE_STATEMENT                  = 2
	CONNECTION_CREATE_BLOB                       = 3
	CONNECTION_CREATE_CLOB                       = 4
	CLOSE_STATEMENT                              = 5
	EXECUTE_STATEMENT                            = 6
	EXECUTE_BATCH                                = 7
	CONNECTION_PREPARED_STATEMENT                = 8
	CLOSE_PREPARED_STATEMENT                     = 9
	EXECUTE_PREPARED_STATEMENT                   = 10
	RESULT_SET_QUERY_NEXT                        = 11
	RESULT_SET_QUERY_PREV                        = 12
	RESULT_SET_RESOVE_LARGE_STRING_REF           = 13
	RESULT_SET_CLOSE                             = 14
	DATABASE_META_DATA_GET_SCHEMAS               = 15
	DATABASE_META_DATA_GET_TABLESPACES           = 16
	DATABASE_META_DATA_GET_TABLES                = 17
	DATABASE_META_DATA_GET_TABLE_PRIVILEGES      = 18
	DATABASE_META_DATA_GET_USER_TABLE_PRIVILEGES = 19
	DATABASE_META_DATA_GET_COLUMNS               = 20
	DATABASE_META_DATA_GET_PRIMARY_KEYS          = 21
	DATABASE_META_DATA_GET_EXPORTED_KEYS         = 22
	DATABASE_META_DATA_GET_IMPORTED_KEYS         = 23
	DATABASE_META_DATA_GET_CATALOGS              = 24
	DATABASE_META_DATA_GET_USERS                 = 25
	BLOB_GET_BINARY_STREAM                       = 26
	CONNECTION_COMMIT                            = 27
	CONNECTION_ROLLBACK                          = 28
	LOB_READ_BUFFER                              = 29
	LOB_WRITE_BUFFER                             = 30
	LOB_GET_DATA_BLOCK_INFO                      = 31
	BLOB_LENGTH                                  = 32
	BLOB_GET_BYTES                               = 33
	BLOB_POSITION_BYTEARRAY_PATTERN              = 34
	BLOB_POSITION_BLOB_PATTERN                   = 35
	BLOB_SET_BYTES                               = 36
	BLOB_SET_BINARY_STREAM                       = 37
	BLOB_TRUNCATE                                = 38
	BLOB_FREE                                    = 39
	CLOB_FREE                                    = 40
	CLOB_READ                                    = 41
	CLOB_WRITE                                   = 42
	CLOB_GET_ASCII_STREAM                        = 43
	CLOB_GET_CHARACTER_STREAM                    = 44
	CLOB_GET_SUB_STRING                          = 45
	CLOB_LENGTH                                  = 46
	CLOB_POSITION_STRING                         = 47
	CLOB_POSITION_CLOB                           = 48
	CLOB_SET_ASCII_STREAM                        = 49
	CLOB_SET_CHARACTER_STREAM                    = 50
	CLOB_SET_STRING                              = 51
	CLOB_TRUNCATE                                = 52
	DISPLAY                                      = 53
	CONNECTION_SHUT_DOWN_SERVER                  = 54
	CLEAR_CACHE                                  = 55
	DATABASE_META_DATA_LIST_SCHEMAS              = 56
	DATABASE_META_DATA_LIST_TABLES               = 57
	EXECUTE_STATEMENT_BATCH_INSERT               = 58
	CONNECTION_CREATE_BLOBS                      = 59
	CONNECTION_CREATE_CLOBS                      = 60
	CONNECTION_SET_TABLET_SPLIT_THRESHOLD        = 61
	EXECUTE_BATCH_PREPARED                       = 62
	DATABASE_META_DATA_GET_TYPE_INFO             = 63
	CREATE_FULL_TEXT_INDEX                       = 64
	FULL_TEXT_SEARCH                             = 65
	DELETE_FULL_TEXT_INDEX                       = 66
	HIGHLIGHT                                    = 67
	RESULT_SET_GET_RECORD_COUNT                  = 68
	RESULT_SET_GET_EXECUTION_INFO                = 69
	DATABASE_META_DATA_GET_SERVERS               = 70
	DATABASE_META_DATA_GET_TABLETS               = 71
	DATABASE_META_DATA_GET_SEQUENCES             = 72
	DATA_LOAD                                    = 73
	CHECK_POINT                                  = 74
	GET_TABLET_RESULT_SET                        = 75
	GET_INFO_FROM_HDFS                           = 76
	GET_CPU_INFO                                 = 77
	GET_HDFS_DATA_BLOCK_SIZE                     = 78

	EXECUTE_GC                                         = 79
	GET_RUNNING_SQL                                    = 80
	GET_RUNNING_TASK                                   = 81
	GET_ONLINE_USER                                    = 82
	GET_CACHE                                          = 83
	SET_TRANSACTION_ISOLATION                          = 84
	CREATE_UDF                                         = 85
	DELETE_UDF                                         = 86
	GET_UDF_CLASS_NAME                                 = 87
	GET_UDF_METHOD_NAMES                               = 88
	CONNECTION_CALLABLE_STATEMENT                      = 89
	EXECUTE_CALLABLE_STATEMENT                         = 90
	DATABASE_META_DATA_GET_RECORD_COUNT_OF_ALL_TABLETS = 91
	DATABASE_META_DATA_GET_TABLET_COUNT                = 92
	GET_THREAD_INFO                                    = 93
	GET_FULLTEXTINDEX_INFO                             = 94
	SET_AUTO_TABLET_RECORDCOUNT                        = 95
	RESULT_SET_GET_TABLET_IDS                          = 96
	GET_INC_LOGS                                       = 97
	REDO_INC_LOGS                                      = 98
	DATABASE_META_DATA_GET_DB_FILES                    = 99
	DATABASE_META_DATA_GET_DB_FILE_DATA                = 100
	INSERT_DB_FILE_DATA                                = 101
	CLOSE_DB_FILE_OUTPUT                               = 102
	GET_SERVER_VERSION                                 = 103
	CONNECTION_CANCEL_STATEMENT                        = 104
	GET_TRANSACTION_ISOLATION                          = 105
	DATABASE_META_DATA_GET_SYSTEM_UTILIZATION          = 106
	DATABASE_META_DATA_GET_MEMORY_SIZE                 = 107
	GET_CONFIG_OPTIONS                                 = 108
	GET_SYSTEM_OVERVIEW                                = 109
	CONNECTION_SET_CLIENT_PROPERTIES                   = 110
	DATABASE_META_DATA_GET_SCHEMA_OWNER                = 111
	DATABASE_UPDATE_PATCH                              = 112
	DATABASE_RESTART_SERVER                            = 113
	DATABASE_META_DATA_GET_USER_PRIVILEGES             = 114
	GET_INDEX_INFO                                     = 115
	DATABASE_UPDATE_LICENSE                            = 116
	GET_BFILE_TABLE_TOTAL_LENGTH                       = 117
	GET_BFILE_CONTENT_TABLE_TOTAL_LENGTH               = 118
	GET_BFILE_CONTENT_TABLE_EVERY_LENGTH               = 119
	GET_INFO_FOR_MAP_REDUCE                            = 120
	IS_BFILE_UFS_STORE                                 = 121
	EXECUTE_STATEMENT_4_MR                             = 122
	GET_TEXTINDEX_INFO                                 = 123
	DATABASE_META_DATA_GET_UNIQUE_KEYS                 = 124
	RELOAD_CONFIGURATION                               = 125
	CONNECTION_SET_CHECK_CONSTRAINTS                   = 126
	TABLE_GET_RECORDS_BY_PKS                           = 127
	DATABASE_GET_SQL_HISTORYS                          = 128
	DATABASE_SET_SHARE_QUERYAREA                       = 129
	DATABASE_GET_RUNTIME_REPORT                        = 130
	UPDATE_CONFIG_OPTIONS                              = 131
	DATABASE_META_DATA_GET_NETWORK_STATUS              = 132
	DATABASE_HEALTH_DIAGNOSTIC                         = 133
	DATABASE_META_DATA_GET_COLUMNS_DEFAULT             = 134
	DATABASE_GET_SQL_STATISTICS                        = 135
	RESULT_SET_GET_DISTRIBUTION                        = 136
	RESULT_SET_GET_EXECUTION_STATISTICS                = 137
	RESULT_SET_GET_TABLET_PARTITION_IDS                = 138
	AUTO_TABLET_APPEND                                 = 139
	GET_SERVER_LOGGER                                  = 140
	DATABASE_COLLECT_LOGGER                            = 141
	DATABASE_META_DATA_GET_TABLE_TYPES                 = 142
	SET_FULLTEXT_INDEX_IS_AND_OPERATOR                 = 143
	GET_FULLTEXT_INDEX_IS_AND_OPERATOR                 = 144
	DATABASE_META_DATA_GET_SYNONYM_COLUMNS             = 145
	GET_SYNONYM_HINTS                                  = 146
	CONNECTION_SET_ENABLE_SAME_COLUMN_LINK             = 147
	GET_ALL_DOWNLEVELS                                 = 148
	DATABASE_META_DATA_GET_TABLE_SYNONYMS              = 149
	DATABASE_META_DATA_GET_TABLE_BASESEARCH_COLUMNS    = 150
	DATABASE_META_DATA_GET_LINK_KEYS                   = 151
	GET_DOWNLEVEL_CHAINS                               = 152
	DATABASE_META_DATA_GET_SCHEMA_FILES                = 153
	REFRESH                                            = 154
	SET_SCHEMA                                         = 155
	DATABASE_META_DATA_GET_SHARES                      = 156
	GET_ZONE_SERVERS                                   = 157
	CREATE_TABLET                                      = 158
)
