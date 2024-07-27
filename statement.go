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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

type cwStmt struct {
	mc              *cwConn
	stmtType        byte
	id              uint32
	executeSequence int
	cursorId        int32
	paramCount      int
	paramType       []byte
	autokeyFields   []bool
}

func (stmt *cwStmt) Close() error {
	if stmt.mc == nil || stmt.mc.closed.IsSet() {
		// driver.Stmt.Close can be called more than once, thus this function
		// has to be idempotent.
		// See also Issue #450 and golang/go#16019.
		//errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}

	var err error
	pktLen := 25 + 4
	data, err := stmt.mc.buf.takeBuffer(pktLen)
	if err != nil {
		return err
	}

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(stmt.id))
	pos += 4
	//	if stmt.stmtType == CONNECTION_PREPARED_STATEMENT {
	//		err = stmt.mc.setCommandPacket(CLOSE_PREPARED_STATEMENT, pos, data[0:pos])
	//	} else {
	err = stmt.mc.setCommandPacket(CLOSE_STATEMENT, pos, data[0:pos])
	//	}
	//	if stmt.stmtType != CONNECTION_PREPARED_STATEMENT {
	err = stmt.mc.writePacket(data[0:pos])
	if err == nil {
		_, err = stmt.mc.readResultOK()
	}
	//	}
	stmt.mc = nil
	return err
}

func (stmt *cwStmt) NumInput() int {
	return stmt.paramCount
}

func (stmt *cwStmt) ColumnConverter(idx int) driver.ValueConverter {
	return converter{}
}

func (stmt *cwStmt) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = converter{}.ConvertValue(nv.Value)
	return
}

func (stmt *cwStmt) Exec(args []driver.Value) (driver.Result, error) {
	if stmt.mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	var err error
	// Send command
	if stmt.mc.txBatchFlag {
		err = stmt.writeTxBatchExecutePacket(args)
		fmt.Println("txBatchFlag true")
		if err != nil {
			return nil, stmt.mc.markBadConn(err)
		}
		mc := stmt.mc

		mc.affectedRows = 0
		mc.insertId = 0

		// Read Result
		var res []int64
		res, err = mc.readCloudResultSetPacket()
		if err != nil {
			return nil, err
		}
		if len(res) > 0 {
			// Columns
			if err = mc.readUntilEOF(); err != nil {
				return nil, err
			}

			// Rows
			if err := mc.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		if err := mc.discardResults(); err != nil {
			return nil, err
		}

		return &cwResult{
			affectedRows: int64(mc.affectedRows),
			insertId:     int64(mc.insertId),
		}, nil
	} else {
		err = stmt.writeExecutePacket(int(stmt.mc.execType), args)
		fmt.Println("txBatchFlag false")
		if err != nil {
			return nil, stmt.mc.markBadConn(err)
		}
		mc := stmt.mc

		mc.affectedRows = 0
		mc.insertId = 0

		// Read Result
		var resLen int
		resLen, _, err = stmt.readResultSetHeaderPacket2()
		if err != nil {
			return nil, err
		}
		if resLen > 0 {
			// Columns
			if err = mc.readUntilEOF(); err != nil {
				return nil, err
			}

			// Rows
			if err := mc.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		if err := mc.discardResults(); err != nil {
			return nil, err
		}

		return &cwResult{
			affectedRows: int64(mc.affectedRows),
			insertId:     int64(mc.insertId),
		}, nil
	}
}

func (stmt *cwStmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.query(args)
}

// func (stmt *cwStmt) query(args []driver.Value) (*binaryRows, error) {
func (stmt *cwStmt) query(args []driver.Value) (*textRows, error) {
	if stmt.mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// Send command
	err := stmt.writeExecutePacket(CLOUDWAVE_EXECUTE_QUERY, args)
	if err != nil {
		return nil, stmt.mc.markBadConn(err)
	}

	// Read Result
	resLen, rows, err := stmt.readResultSetHeaderPacket2()
	if err != nil {
		return nil, err
	}
	if resLen <= 0 {
		rows.rs.done = true

		switch err := rows.NextResultSet(); err {
		case nil, io.EOF:
			return rows, nil
		default:
			return nil, err
		}
	}
	return rows, err
}

var jsonType = reflect.TypeOf(json.RawMessage{})

type converter struct{}

// ConvertValue mirrors the reference/default converter in database/sql/driver
// with _one_ exception.  We support uint64 with their high bit and the default
// implementation does not.  This function should be kept in sync with
// database/sql/driver defaultConverter.ConvertValue() except for that
// deliberate difference.
func (c converter) ConvertValue(v interface{}) (driver.Value, error) {
	if driver.IsValue(v) {
		return v, nil
	}

	if vr, ok := v.(driver.Valuer); ok {
		sv, err := callValuerValue(vr)
		if err != nil {
			return nil, err
		}
		if driver.IsValue(sv) {
			return sv, nil
		}
		// A value returend from the Valuer interface can be "a type handled by
		// a database driver's NamedValueChecker interface" so we should accept
		// uint64 here as well.
		if u, ok := sv.(uint64); ok {
			return u, nil
		}
		return nil, fmt.Errorf("non-Value type %T returned from Value", sv)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		} else {
			return c.ConvertValue(rv.Elem().Interface())
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint(), nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.Slice:
		switch t := rv.Type(); {
		case t == jsonType:
			return v, nil
		case t.Elem().Kind() == reflect.Uint8:
			return rv.Bytes(), nil
		default:
			return nil, fmt.Errorf("unsupported type %T, a slice of %s", v, t.Elem().Kind())
		}
	case reflect.String:
		return rv.String(), nil
	}
	return nil, fmt.Errorf("unsupported type %T, a %s", v, rv.Kind())
}

var valuerReflectType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

// callValuerValue returns vr.Value(), with one exception:
// If vr.Value is an auto-generated method on a pointer type and the
// pointer is nil, it would panic at runtime in the panicwrap
// method. Treat it like nil instead.
//
// This is so people can implement driver.Value on value types and
// still use nil pointers to those types to mean nil/NULL, just like
// string/*string.
//
// This is an exact copy of the same-named unexported function from the
// database/sql package.
func callValuerValue(vr driver.Valuer) (v driver.Value, err error) {
	if rv := reflect.ValueOf(vr); rv.Kind() == reflect.Ptr &&
		rv.IsNil() &&
		rv.Type().Elem().Implements(valuerReflectType) {
		return nil, nil
	}
	return vr.Value()
}
