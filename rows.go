// Go CloudWave Driver - A CloudWave-Driver for Go's database/sql package
//
// Copyright 2012 The Go-CloudWave-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package cloudwave

import (
	"database/sql/driver"
	"io"
	"math"
//	"reflect"
)

type resultSet struct {
	columns     []cwField
	columnNames []string
	done        bool
}

type cwRows struct {
//	mc     *cwConn
	stmt   *cwStmt
	rs     resultSet
	finish func()

	stmtId			int32
	cursorId		int32
	isQuery			byte
	resultCount     int64
}

type binaryRows struct {
	cwRows
}

type textRows struct {
	cwRows
}

func (rows *cwRows) Columns() []string {
	if rows.rs.columnNames != nil {
		return rows.rs.columnNames
	}

	columns := make([]string, len(rows.rs.columns))
	if rows.stmt.mc != nil && rows.stmt.mc.cfg.ColumnsWithAlias {
		for i := range columns {
			if tableName := rows.rs.columns[i].tableName; len(tableName) > 0 {
				columns[i] = tableName + "." + rows.rs.columns[i].name
			} else {
				columns[i] = rows.rs.columns[i].name
			}
		}
	} else {
		for i := range columns {
			columns[i] = rows.rs.columns[i].name
		}
	}

	rows.rs.columnNames = columns
	return columns
}

func (rows *cwRows) ColumnTypeDatabaseTypeName(i int) string {
	return rows.rs.columns[i].typeDatabaseName()
}

func (rows *cwRows) ColumnTypeNullable(i int) (nullable, ok bool) {
	return rows.rs.columns[i].flags&flagNotNULL == 0, true
}

func (rows *cwRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	column := rows.rs.columns[i]
	decimals := int64(column.decimals)

	switch column.fieldType {
	case CLOUD_TYPE_TINY_DECIMAL, CLOUD_TYPE_SMALL_DECIMAL, CLOUD_TYPE_BIG_DECIMAL:
		if decimals > 0 {
			return int64(column.length) - 2, decimals, true
		}
		return int64(column.length) - 1, decimals, true
	case CLOUD_TYPE_TIMESTAMP, CLOUD_TYPE_TIME:
		return decimals, decimals, true
	case CLOUD_TYPE_FLOAT, CLOUD_TYPE_DOUBLE:
		if decimals == 0x1f {
			return math.MaxInt64, math.MaxInt64, true
		}
		return math.MaxInt64, decimals, true
	}

	return 0, 0, false
}

func (rows *cwRows) Close() (err error) {
	if f := rows.finish; f != nil {
		f()
		rows.finish = nil
	}

	mc := rows.stmt.mc
	if mc == nil {
		rows.stmt.Close()
		return nil
	}
	if err := mc.error(); err != nil {
		return err
	}

	// flip the buffer for this connection if we need to drain it.
	// note that for a successful query (i.e. one where rows.next()
	// has been called until it returns false), `rows.mc` will be nil
	// by the time the user calls `(*Rows).Close`, so we won't reach this
	// see: https://github.com/golang/go/commit/651ddbdb5056ded455f47f9c494c67b389622a47
	mc.buf.flip()

	// Remove unread packets from stream
	if !rows.rs.done {
		err = mc.readUntilEOF()
	}
	if err == nil {
		if err = mc.discardResults(); err != nil {
			return err
		}
	}

	rows.stmt.Close()
	rows.stmt.mc = nil
	return err
}

func (rows *cwRows) HasNextResultSet() (b bool) {
	if rows.stmt.mc == nil {
		return false
	}
	return rows.stmt.mc.status&statusMoreResultsExists != 0
}

func (rows *cwRows) nextResultSet() (int, error) {
	if rows.stmt.mc == nil {
		return 0, io.EOF
	}
	if err := rows.stmt.mc.error(); err != nil {
		return 0, err
	}

	// Remove unread packets from stream
	if !rows.rs.done {
		if err := rows.stmt.mc.readUntilEOF(); err != nil {
			return 0, err
		}
		rows.rs.done = true
	}

	if !rows.HasNextResultSet() {
		rows.stmt.mc = nil
		return 0, io.EOF
	}
	rows.rs = resultSet{}
	return rows.stmt.mc.readResultSetHeaderPacket()
}

func (rows *cwRows) nextNotEmptyResultSet() (int, error) {
	for {
		resLen, err := rows.nextResultSet()
		if err != nil {
			return 0, err
		}

		if resLen > 0 {
			return resLen, nil
		}

		rows.rs.done = true
	}
}

func (rows *binaryRows) NextResultSet() error {
	resLen, err := rows.nextNotEmptyResultSet()
	if err != nil {
		return err
	}

	rows.rs.columns, err = rows.stmt.mc.readColumns(resLen)
	return err
}

func (rows *binaryRows) Next(dest []driver.Value) error {
	if mc := rows.stmt.mc; mc != nil {
		if err := mc.error(); err != nil {
			return err
		}

		// Fetch next row from stream
		return rows.readRow(dest)
	}
	return io.EOF
}

func (rows *textRows) NextResultSet() (err error) {
	resLen, err := rows.nextNotEmptyResultSet()
	if err != nil {
		return err
	}

	rows.rs.columns, err = rows.stmt.mc.readColumns(resLen)
	return err
}

func (rows *textRows) Next(dest []driver.Value) error {
	if mc := rows.stmt.mc; mc != nil {
		if err := mc.error(); err != nil {
			return err
		}

		// Fetch next row from stream
		return rows.readRow(dest)
	}
	return io.EOF
}
