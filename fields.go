// Go CloudWave Driver - A CloudWave-Driver for Go's database/sql package
//
// Copyright 2017 The Go-CloudWave-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package cloudwave

/*
	func (mf *cwField) typeDatabaseName() string {
		switch mf.fieldType {
		case CLOUD_TYPE_BYTE:
			return "BYTE"
		case CLOUD_TYPE_BOOLEAN:
			return "BOOLEAN"
		case CLOUD_TYPE_CHAR:
			return "CHAR"
		case CLOUD_TYPE_VARCHAR:
			return "VARCHAR"
		case CLOUD_TYPE_BINARY:
			return "BINARY"
		case CLOUD_TYPE_VARBINARY:
			return "VARBINARY"
		case CLOUD_TYPE_INTEGER:
			return "INTEGER"
		case CLOUD_TYPE_LONG:
			return "LONG"
		case CLOUD_TYPE_FLOAT:
			return "FLOAT"
		case CLOUD_TYPE_DOUBLE:
			return "DOUBLE"
		case CLOUD_TYPE_DATE:
			return "DATE"
		case CLOUD_TYPE_TIME:
			return "TIME"
		case CLOUD_TYPE_TIMESTAMP:
			return "TIMESTAMP"

		case CLOUD_TYPE_BLOB:
			if mf.charSet != collations[binaryCollation] {
				return "TEXT"
			}
			return "BLOB"

		case CLOUD_TYPE_TINY_INTEGER:
			return "TINYINT"
		case CLOUD_TYPE_SMALL_INTEGER:
			if mf.flags&flagUnsigned != 0 {
				return "UNSIGNED SMALLINT"
			}
			return "SMALLINT"
		case CLOUD_TYPE_BIG_INTEGER:
			if mf.flags&flagUnsigned != 0 {
				return "UNSIGNED BIGINT"
			}
			return "BIGINT"

		case CLOUD_TYPE_TINY_DECIMAL:
			if mf.flags&flagUnsigned != 0 {
				return "UNSIGNED TINYINT"
			}
			return "DECIMAL"
		case CLOUD_TYPE_SMALL_DECIMAL:
			return "DECIMAL"
		case CLOUD_TYPE_BIG_DECIMAL:
			return "DECIMAL"
		default:
			return ""
		}
	}
*/
func (mf *cwField) typeDatabaseName() string {
	switch mf.columnHeaderFieldType {
	case BIT:
		return "BOOLEAN"
	case TINYINT:
		return "BOOLEAN"
	case SMALLINT:
		return "SMALLINT"
	case INTEGER:
		return "INTEGER"
	case BIGINT:
		return "LONG"
	case FLOAT:
		return "FLOAT"
	case REAL:
		return "DECIMAL"
	case DOUBLE:
		return "DOUBLE"
	case NUMERIC:
		return "NUMERIC"
	case DECIMAL:
		return "NUMERIC"
	case CHAR:
		return "CHAR"
	case VARCHAR:
		return "VARCHAR"
	case LONGVARCHAR:
		return "CLOB"
	case DATE:
		return "DATE"
	case TIME:
		return "TIME"
	case TIMESTAMP:
		return "TIMESTAMP"
	case BINARY:
		return "BINARY"
	case VARBINARY:
		return "VARBINARY"
	case LONGVARBINARY:
		return "BLOB"
	case NULL: //
		return ""
	case OTHER: //
		return ""
	case JAVA_OBJECT: //
		return ""
	case DISTINCT: //
		return ""
	case STRUCT: //
		return ""
	case ARRAY:
		return "ARRAY"
	case BLOB:
		return "BLOB"
	case CLOB:
		return "CLOB"
	case REF: //
		return ""
	case DATALINK: //
		return ""
	case BOOLEAN:
		return "BOOLEAN"
	case ROWID: //
		return ""
	case NCHAR:
		return "CHAR"
	case NVARCHAR:
		return "VARCHAR"
	case LONGNVARCHAR:
		return "CLOB"
	case NCLOB:
		return "CLOB"
	case SQLXML: //
		return ""
	case REF_CURSOR: //
		return ""
	case TIME_WITH_TIMEZONE: //
		return ""
	case TIMESTAMP_WITH_TIMEZONE: //
		return ""
	default:
		return ""
	}
}

func toCloudType(JdbcType columnHeaderFieldType) byte {
	switch JdbcType {
	case BIT:
		return CLOUD_TYPE_BOOLEAN
	case TINYINT:
		return CLOUD_TYPE_BOOLEAN
	case SMALLINT:
		return CLOUD_TYPE_SMALL_INTEGER
	case REAL:
		return CLOUD_TYPE_BIG_DECIMAL
	case DECIMAL:
		return CLOUD_TYPE_BIG_DECIMAL
	case NULL:
		return CLOUD_TYPE_NULL
	//case JAVA_OBJECT:
	//	return
	//case DISTINCT:
	//	return
	//case STRUCT:
	//	return
	//case REF:
	//	return
	//case REF_CURSOR:
	//	return
	//case DATALINK:
	//	return
	//case NCHAR:
	//	return CLOUD_TYPE_CHAR
	//case NVARCHAR:
	//	return CLOUD_TYPE_VARCHAR
	//case LONGNVARCHAR:
	//	return CLOUD_TYPE_CLOB
	//case NCLOB:
	//	return CLOUD_TYPE_CLOB
	//case SQLXML:
	//	return
	//case TIME_WITH_TIMEZONE:
	//	return
	//case TIMESTAMP_WITH_TIMEZONE:
	//	return

	case ARRAY:
		return CLOUD_TYPE_ARRAY
	case NUMERIC:
		return CLOUD_TYPE_BIG_DECIMAL
	//case NUMERIC:
	//	return CLOUD_TYPE_SMALL_DECIMAL
	//case NUMERIC:
	//	return CLOUD_TYPE_TINY_DECIMAL
	case BINARY:
		return CLOUD_TYPE_BINARY
	case BLOB:
		return CLOUD_TYPE_BLOB
	case BOOLEAN:
		return CLOUD_TYPE_BOOLEAN
	case CHAR:
		return CLOUD_TYPE_CHAR
	case CLOB:
		return CLOUD_TYPE_CLOB
	case DATE:
		return CLOUD_TYPE_DATE
	case DOUBLE:
		return CLOUD_TYPE_DOUBLE
	case FLOAT:
		return CLOUD_TYPE_FLOAT
	case INTEGER:
		return CLOUD_TYPE_INTEGER
	//case INTEGER:
	//	return CLOUD_TYPE_INTERVAL
	//case INTEGER:
	//	return CLOUD_TYPE_TINY_INTEGER
	case BIGINT:
		return CLOUD_TYPE_LONG
	//case BIGINT:
	//	return CLOUD_TYPE_SMALL_INTEGER
	case LONGVARBINARY:
		return CLOUD_TYPE_LONGVARBINARY
	case LONGVARCHAR:
		return CLOUD_TYPE_LONGVARCHAR
	//case BINARY:
	//	return CLOUD_TYPE_SINGLE_BYTE
	//case CHAR:
	//	return CLOUD_TYPE_SINGLE_CHAR
	case TIME:
		return CLOUD_TYPE_TIME
	case TIMESTAMP:
		return CLOUD_TYPE_TIMESTAMP
	case VARBINARY:
		return CLOUD_TYPE_VARBINARY
	case VARCHAR:
		return CLOUD_TYPE_VARCHAR
	case ROWID:
		return CLOUD_TYPE_ROWID
	}
	return byte(CLOUD_TYPE_OTHER & 0xff)
}

func toJdbcType(wisdomType byte) int32 {
	switch wisdomType {
	case CLOUD_TYPE_ARRAY:
		return ARRAY
	case CLOUD_TYPE_BIG_DECIMAL:
		return NUMERIC
	case CLOUD_TYPE_BIG_INTEGER:
		return NUMERIC
	case CLOUD_TYPE_BINARY:
		return BINARY
	case CLOUD_TYPE_BLOB:
		return BLOB
	case CLOUD_TYPE_BOOLEAN:
		return BOOLEAN
	case CLOUD_TYPE_CHAR:
		return CHAR
	case CLOUD_TYPE_CLOB:
		return CLOB
	case CLOUD_TYPE_DATE:
		return DATE
	case CLOUD_TYPE_DOUBLE:
		return DOUBLE
	case CLOUD_TYPE_FLOAT:
		return FLOAT
	case CLOUD_TYPE_INTEGER:
		return INTEGER
	case CLOUD_TYPE_INTERVAL:
		return INTEGER
	case CLOUD_TYPE_LONG:
		return BIGINT
	case CLOUD_TYPE_LONGVARBINARY:
		return LONGVARBINARY
	case CLOUD_TYPE_LONGVARCHAR:
		return LONGVARCHAR
	case CLOUD_TYPE_SINGLE_BYTE:
		return BINARY
	case CLOUD_TYPE_SINGLE_CHAR:
		return CHAR
	case CLOUD_TYPE_SMALL_DECIMAL:
		return NUMERIC
	case CLOUD_TYPE_TIME:
		return TIME
	case CLOUD_TYPE_TIMESTAMP:
		return TIMESTAMP
	case CLOUD_TYPE_TINY_DECIMAL:
		return NUMERIC
	case CLOUD_TYPE_TINY_INTEGER:
		return INTEGER
	case CLOUD_TYPE_SMALL_INTEGER:
		return BIGINT
	case CLOUD_TYPE_VARBINARY:
		return VARBINARY
	case CLOUD_TYPE_VARCHAR:
		return VARCHAR
	case CLOUD_TYPE_ROWID:
		return ROWID
	}
	return OTHER
}

func getTypeName(wisdomType byte) string {
	switch wisdomType {
	case CLOUD_TYPE_ARRAY:
		return "ARRAY"
	case CLOUD_TYPE_BIG_DECIMAL:
		return "NUMERIC"
	case CLOUD_TYPE_BIG_INTEGER:
		return "NUMERIC"
	case CLOUD_TYPE_BINARY:
		return "BINARY"
	case CLOUD_TYPE_BLOB:
		return "BLOB"
	case CLOUD_TYPE_BOOLEAN:
		return "BOOLEAN"
	case CLOUD_TYPE_CHAR:
		return "CHAR"
	case CLOUD_TYPE_CLOB:
		return "CLOB"
	case CLOUD_TYPE_DATE:
		return "DATE"
	case CLOUD_TYPE_DOUBLE:
		return "DOUBLE"
	case CLOUD_TYPE_FLOAT:
		return "FLOAT"
	case CLOUD_TYPE_TINY_INTEGER, CLOUD_TYPE_INTEGER:
		return "INTEGER"
	case CLOUD_TYPE_INTERVAL:
		return "INTEGER"
	case CLOUD_TYPE_LONG, CLOUD_TYPE_SMALL_INTEGER:
		return "BIGINT"
	case CLOUD_TYPE_LONGVARBINARY:
		return "LONGVARBINARY"
	case CLOUD_TYPE_LONGVARCHAR:
		return "LONGVARCHAR"
	case CLOUD_TYPE_SINGLE_BYTE:
		return "BINARY"
	case CLOUD_TYPE_SINGLE_CHAR:
		return "CHAR"
	case CLOUD_TYPE_SMALL_DECIMAL:
		return "NUMERIC"
	case CLOUD_TYPE_NUMBER:
		return "NUMERIC"
	case CLOUD_TYPE_TIME:
		return "TIME"
	case CLOUD_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case CLOUD_TYPE_TINY_DECIMAL:
		return "NUMERIC"
	case CLOUD_TYPE_VARBINARY:
		return "VARBINARY"
	case CLOUD_TYPE_VARCHAR:
		return "VARCHAR"
	}
	return "OTHER"
}

type cwField struct {
	tableName string
	name      string
	length    uint32
	flags     fieldFlag
	fieldType fieldType
	decimals  byte
	charSet   uint8

	columnHeaderFieldType columnHeaderFieldType
	colScale              uint32
	typeName              string
	className             string
}
