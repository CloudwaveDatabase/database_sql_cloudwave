// Go CloudWave Driver - A CloudWave-Driver for Go's database/sql package
//
// Copyright 2017 The Go-CloudWave-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package cloudwave

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

type cwField struct {
	tableName string
	name      string
	length    uint32
	flags     fieldFlag
	fieldType fieldType
	decimals  byte
	charSet   uint8

	colScale  uint32
	typeName  string
	className string
}
