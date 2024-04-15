// Go CloudWave Driver - A CloudWave-Driver for Go's database/sql package
//
// Copyright 2012 The Go-CloudWave-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package cloudwave

import (
	//	"bytes"
	"crypto/sha1"
	"regexp"

	//	"strings"
	//	"crypto/tls"
	"database/sql/driver"
	"encoding/base64"
	"encoding/binary"
	//	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	//	"strconv"
	"time"
)

// Packets documentation:
// http://dev.cloudwave.com/doc/internals/en/client-server-protocol.html

func (mc *cwConn) requestServer(requestType int) ([]byte, error) {
	data, err := mc.buf.takeSmallBuffer(25)
	if err != nil {
		return nil, err
	}
	return mc.requestServer1(requestType, data)
}

func (mc *cwConn) requestServer1(requestType int, outdata []byte) ([]byte, error) {

	pktLen := len(outdata)
	if pktLen < 25 {
		return nil, ErrInvalidConn
	}
	outdata[0] = B_REQ_TAG
	binary.BigEndian.PutUint32(outdata[1:], uint32(pktLen-5))
	binary.BigEndian.PutUint32(outdata[5:], uint32(requestType))
	binary.BigEndian.PutUint64(outdata[9:], uint64(mc.sessionTime))
	binary.BigEndian.PutUint64(outdata[17:], uint64(mc.sessionSequence))
	if err := mc.writePacket(outdata); err != nil {
		return nil, ErrInvalidConn
	}

	indata, err := mc.readPacket()
	if err != nil {
		return nil, err
	}
	return indata, nil
}

// Read packet to buffer 'data'
func (mc *cwConn) readPacket() ([]byte, error) {
	var prevData []byte
	for {
		// read packet header
		data, err := mc.buf.readNext(4)
		if err != nil {
			if cerr := mc.canceled.Value(); cerr != nil {
				return nil, cerr
			}
			errLog.Print(err)
			mc.Close()
			return nil, ErrInvalidConn
		}

		// packet length [24 bit]
		//		pktLen := int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16)
		pktLen := int(binary.BigEndian.Uint32(data[0:])) - 4

		/*		// check packet sync [8 bit]
				if data[3] != mc.sequence {
					if data[3] > mc.sequence {
						return nil, ErrPktSyncMul
					}
					return nil, ErrPktSync
				}
				mc.sequence++

				// packets with length 0 terminate a previous packet which is a
				// multiple of (2^24)-1 bytes long
				if pktLen == 0 {
					// there was no previous packet
					if prevData == nil {
						errLog.Print(ErrMalformPkt)
						mc.Close()
						return nil, ErrInvalidConn
					}

					return prevData, nil
				}
		*/
		// read packet body [pktLen bytes]
		data, err = mc.buf.readNext(pktLen)
		if err != nil {
			if cerr := mc.canceled.Value(); cerr != nil {
				return nil, cerr
			}
			errLog.Print(err)
			mc.Close()
			return nil, ErrInvalidConn
		}

		// return data if this was the last packet
		if pktLen < maxPacketSize {
			// zero allocations for non-split packets
			if prevData == nil {
				return data, nil
			}

			return append(prevData, data...), nil
		}

		prevData = append(prevData, data...)
	}
}

// Write packet buffer 'data'
func (mc *cwConn) writePacket(data []byte) error {
	pktLen := len(data)
	if pktLen <= 0 {
		return nil
	}
	if pktLen > mc.maxAllowedPacket {
		return ErrPktTooLarge
	}

	// Perform a stale connection check. We only perform this check for
	// the first query on a connection that has been checked out of the
	// connection pool: a fresh connection from the pool is more likely
	// to be stale, and it has not performed any previous writes that
	// could cause data corruption, so it's safe to return ErrBadConn
	// if the check fails.
	if mc.reset {
		mc.reset = false
		conn := mc.netConn
		if mc.rawConn != nil {
			conn = mc.rawConn
		}
		var err error
		// If this connection has a ReadTimeout which we've been setting on
		// reads, reset it to its default value before we attempt a non-blocking
		// read, otherwise the scheduler will just time us out before we can read
		if mc.cfg.ReadTimeout != 0 {
			err = conn.SetReadDeadline(time.Time{})
		}
		if err == nil && mc.cfg.CheckConnLiveness {
			err = connCheck(conn)
		}
		if err != nil {
			errLog.Print("closing bad idle connection: ", err)
			mc.Close()
			return driver.ErrBadConn
		}
	}

	for {
		// Write packet
		if mc.writeTimeout > 0 {
			if err := mc.netConn.SetWriteDeadline(time.Now().Add(mc.writeTimeout)); err != nil {
				return err
			}
		}

		n, err := mc.netConn.Write(data[:pktLen])
		if err == nil {
			if n == pktLen {
				mc.sequence++
				return nil
			} else {
				mc.cleanup()
				errLog.Print(ErrMalformPkt)
			}
		} else {
			if cerr := mc.canceled.Value(); cerr != nil {
				return cerr
			}
			if n == 0 {
				// only for the first loop iteration when nothing was written yet
				return errBadConnNoWrite
			}
			mc.cleanup()
			errLog.Print(err)
		}
		return ErrInvalidConn
	}
}

/******************************************************************************
*                           Initialization Process                            *
******************************************************************************/

func (mc *cwConn) readFirstResponsePacket() error {
	data, err := mc.readPacket()
	if err != nil {
		return err
	}
	datalen := len(data)
	if datalen > 0 {
		if data[0] == iOK {
			if datalen >= 25 {
				mc.sessionTime = binary.BigEndian.Uint64(data[1:])
				mc.sessionSequence = binary.BigEndian.Uint64(data[9:])
				mc.sessionToken = binary.BigEndian.Uint64(data[17:])
			}
			return nil
		}
	}
	return errBadConnNoWrite
}

func (mc *cwConn) writeFirstPacket() error {
	//
	timeZoneId := "Asia/Shanghai"

	// shaPwd = SHA1(mc.cfg.Passwd)
	crypt := sha1.New()
	crypt.Write([]byte(mc.cfg.Passwd))
	shaPwd := crypt.Sum(nil)

	encPwd := base64.StdEncoding.EncodeToString(shaPwd)

	pktLen := 1 + 4 + 4 + (4 * 3) + len(mc.cfg.User) + len(encPwd) + len(timeZoneId) + 4

	data, err := mc.buf.takeSmallBuffer(pktLen)
	if err != nil {
		return err
	}

	pos := 9

	length := len(mc.cfg.User)
	binary.BigEndian.PutUint32(data[pos:], uint32(length))
	pos += 4
	if length > 0 {
		pos += copy(data[pos:], mc.cfg.User)
	}

	length = len(encPwd)
	binary.BigEndian.PutUint32(data[pos:], uint32(length))
	pos += 4
	if length > 0 {
		pos += copy(data[pos:], encPwd)
	}

	length = len(timeZoneId)
	binary.BigEndian.PutUint32(data[pos:], uint32(length))
	pos += 4
	if length > 0 {
		pos += copy(data[pos:], timeZoneId)
	}
	data[0] = B_REQ_TAG
	binary.BigEndian.PutUint32(data[1:], uint32(pos-5))
	cmd := B_REQ_BUILD_CONNECTION
	binary.BigEndian.PutUint32(data[5:], uint32(cmd))

	return mc.writePacket(data[:pos])
	return nil
}

/******************************************************************************
*                             Command Packets                                 *
******************************************************************************/

func (mc *cwConn) writeCommandPacket(command int) error {
	// Reset Packet Sequence
	mc.sequence = 0

	data, err := mc.buf.takeSmallBuffer(25)
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return errBadConnNoWrite
	}

	mc.setCommandPacket(command, 25, data[0:25])

	// Send CMD packet
	return mc.writePacket(data)
}

func (mc *cwConn) setCommandPacket(command int, length int, data []byte) error {
	data[0] = B_REQ_TAG
	binary.BigEndian.PutUint32(data[1:], uint32(length-5))
	binary.BigEndian.PutUint32(data[5:], uint32(command))
	binary.BigEndian.PutUint64(data[9:], uint64(mc.sessionTime))
	binary.BigEndian.PutUint64(data[17:], uint64(mc.sessionSequence))
	return nil
}

func (stmt *cwStmt) writeCommandPacketStr(command byte, arg string) error {
	// Reset Packet Sequence
	stmt.mc.sequence = 0

	pktLen := 25 + 4*2 + 4 + len(arg) + 2*4

	data, err := stmt.mc.buf.takeBuffer(pktLen)
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return err
	}

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(stmt.id))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(stmt.executeSequence))
	pos += 4
	stmt.executeSequence++
	binary.BigEndian.PutUint32(data[pos:], uint32(len(arg)))
	pos += 4
	pos += copy(data[pos:], arg)
	binary.BigEndian.PutUint32(data[pos:], uint32(command))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(2)) //NO_GENERATED_KEYS=2
	pos += 4

	stmt.mc.setCommandPacket(EXECUTE_STATEMENT, pos, data[0:25])

	// Send CMD packet
	return stmt.mc.writePacket(data)
}

/******************************************************************************
*                              Result Packets                                 *
******************************************************************************/

// Returns error if Packet is not an 'Result OK'-Packet
func (mc *cwConn) readResultOK() ([]byte, error) {
	data, err := mc.readPacket()
	if err != nil {
		return nil, err
	}
	length := len(data)
	if length > 0 {
		buf := make([]byte, len(data))
		copy(buf, data[0:length])
		if data[0] == iOK {
			return buf, nil
		}
		return nil, mc.handleErrorPacket(data)
	}
	return nil, errors.New("error Result size is 0")
}

func splitName(name string) (string, string) {
	table := ""
	column := ""
	length := len(name)
	i := 0
	for i < length {
		if name[i] == '.' {
			break
		}
		i++
	}
	if i < length {
		table = name[:i]
		column = name[i+1:]
	} else {
		column = name
	}
	return table, column
}

// Result Set Header Packet
// http://dev.cloudwave.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
func (stmt *cwStmt) readResultSetHeaderPacket2() (int, *textRows, error) {
	data, err := stmt.mc.readPacket()
	if err != nil {
		return 0, nil, err
	}
	datalen := len(data)
	if data[0] != iOK {
		return 0, nil, stmt.mc.handleErrorPacket(data)
	}
	if datalen >= 29 {
		var n int
		rows := new(textRows)
		rows.stmt = stmt
		rows.rs.columns = []cwField{}
		localSessionTime := binary.BigEndian.Uint64(data[1:])
		localSessionSequence := binary.BigEndian.Uint64(data[9:])
		sId := uint32(binary.BigEndian.Uint32(data[17:]))
		//上边读到的三个值需要和 stmt 进行比较
		if localSessionTime != stmt.mc.sessionTime || localSessionSequence != stmt.mc.sessionSequence {
			//return 0, nil, errSReadResult
		}
		if sId != rows.stmt.id {
			//return 0, nil, errSReadResult
		}
		rows.cursorId = int32(binary.BigEndian.Uint32(data[21:]))
		rows.stmt.mc.affectedRows = uint64(binary.BigEndian.Uint32(data[25:]))

		pos := 29
		if pos < datalen {
			// read is query
			rows.isQuery = data[pos]
			pos++
		} else {
			return 0, rows, nil
		}
		if pos < datalen {
			// read nullable correlation name
			if data[pos] == 0 {
				var correlationName []byte
				correlationName, _, n, err = ReadLengthEncodedString(data[pos+1:])
				if len(correlationName) > 0 {

				}
				pos += n
			}
			pos++
		} else {
			return 0, rows, nil
		}
		if pos < datalen {
			// read meta data of server cursor's all columns
			// first read column count
			columnSize := int(binary.BigEndian.Uint32(data[pos:]))
			pos += 4
			// extend enough columns
			//CI_set_num_fields(res->fields, columnSize, TRUE);
			if pos < datalen {
				columns := make([]cwField, columnSize)
				// read datas into the column
				j := 0
				for i := 0; i < columnSize; i++ {
					var bytes []byte
					if data[pos] == 0 {
						bytes, _, n, err = ReadLengthEncodedString(data[pos+1:])
						//columns[j].name = string(bytes)
						columns[j].tableName, columns[j].name = splitName(string(bytes))
						pos += n
					}
					pos++
					columns[j].columnHeaderFieldType = columnHeaderFieldType(binary.BigEndian.Uint32(data[pos:]))
					columns[j].fieldType = fieldType(toCloudType(columns[j].columnHeaderFieldType))
					pos += 4

					if data[pos] == 0 {
						bytes, _, n, err = ReadLengthEncodedString(data[pos+1:])
						columns[j].typeName = string(bytes)
						pos += n
					}
					pos++

					columns[j].decimals = byte(binary.BigEndian.Uint32(data[pos:]))
					pos += 4
					columns[j].colScale = binary.BigEndian.Uint32(data[pos:])
					pos += 4

					if data[pos] == 0 {
						bytes, _, n, err = ReadLengthEncodedString(data[pos+1:])
						columns[j].className = string(bytes)
						pos += n
					}
					pos++
					isautokay, _ := regexp.MatchString("__WISDOM_AUTO_KEY__$", columns[j].name)
					if !isautokay {
						j++
					}
				}
				if pos <= len(data) {
					if j == columnSize {
						rows.rs.columns = columns
					} else {
						columnsnew := make([]cwField, j)
						columnsnew = columns[0:j]
						rows.rs.columns = columnsnew
					}
					return columnSize, rows, nil
				}
				// column count
				//			num, _, n := readLengthEncodedInteger(data)
				return 0, nil, ErrMalformPkt
			} else {
				return columnSize, rows, nil
			}
		} else {
			return 0, rows, nil
		}
	}
	return 0, nil, err
}

// Result Set Header Packet
// http://dev.cloudwave.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
func (mc *cwConn) readResultSetHeaderPacket() (int, error) {
	data, err := mc.readPacket()
	if err == nil {
		switch data[0] {

		case iOK:
			return 0, mc.handleOkPacket(data)

		case iERR:
			return 0, mc.handleErrorPacket(data)
		}

		// column count
		num, _, n := readLengthEncodedInteger(data)
		if n-len(data) == 0 {
			return int(num), nil
		}

		return 0, ErrMalformPkt
	}
	return 0, err
}

func (mc *cwConn) readCloudResultSetPacket() ([]int64, error) {
	data, err := mc.readPacket()
	if err == nil {
		datalen := len(data)
		if data[0] == iOK && datalen >= 17 {
			localSessionTime := binary.BigEndian.Uint64(data[1:])
			localSessionSequence := binary.BigEndian.Uint64(data[9:])
			if localSessionTime != mc.sessionTime || localSessionSequence != mc.sessionSequence {
				//				return nil, ErrMalformPkt
			}
			n := (datalen - 17) / 8
			if n > 0 {
				res := make([]int64, n)
				for i := 0; i < n; i++ {
					res[i] = int64(binary.BigEndian.Uint64(data[17+i*8:]))
				}
				return res, nil
			}
			return nil, nil
		}
		err = errors.New("readCloudResultSetPacket error")
	}
	return nil, err
}

// Error Packet
// http://dev.cloudwave.com/doc/internals/en/generic-response-packets.html#packet-ERR_Packet
func (mc *cwConn) handleErrorPacket(data []byte) error {
	if data[0] != iERR {
		return ErrMalformPkt
	}
	pos := 1

	// briefMessage
	length := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	briefmessage := data[pos : pos+length]
	pos += length
	// Message

	length = int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	message := data[pos : pos+length]

	// Error Message [string]
	return &CloudWaveError{
		briefMessage: string(briefmessage),
		Message:      string(message),
	}
}

func readStatus(b []byte) statusFlag {
	return statusFlag(b[0]) | statusFlag(b[1])<<8
}

// Ok Packet
// http://dev.cloudwave.com/doc/internals/en/generic-response-packets.html#packet-OK_Packet
func (mc *cwConn) handleOkPacket(data []byte) error {
	return nil
	var n, m int

	// 0x01 [1 byte]

	// Affected rows [Length Coded Binary]
	mc.affectedRows, _, n = readLengthEncodedInteger(data[1:])

	// Insert id [Length Coded Binary]
	mc.insertId, _, m = readLengthEncodedInteger(data[1+n:])

	// server_status [2 bytes]
	mc.status = readStatus(data[1+n+m : 1+n+m+2])
	if mc.status&statusMoreResultsExists != 0 {
		return nil
	}

	// warning count [2 bytes]

	return nil
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
// http://dev.cloudwave.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41
func (mc *cwConn) readColumns(count int) ([]cwField, error) {
	columns := make([]cwField, count)

	for i := 0; ; i++ {
		data, err := mc.readPacket()
		if err != nil {
			return nil, err
		}

		// EOF Packet
		if data[0] == iEOF && (len(data) == 5 || len(data) == 1) {
			if i == count {
				return columns, nil
			}
			return nil, fmt.Errorf("column count mismatch n:%d len:%d", count, len(columns))
		}

		// Catalog
		pos, err := skipLengthEncodedString(data)
		if err != nil {
			return nil, err
		}

		// Database [len coded string]
		n, err := skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		// Table [len coded string]
		if mc.cfg.ColumnsWithAlias {
			tableName, _, n, err := ReadLengthEncodedString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
			columns[i].tableName = string(tableName)
		} else {
			n, err = skipLengthEncodedString(data[pos:])
			if err != nil {
				return nil, err
			}
			pos += n
		}

		// Original table [len coded string]
		n, err = skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		// Name [len coded string]
		name, _, n, err := ReadLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		columns[i].name = string(name)
		pos += n

		// Original name [len coded string]
		n, err = skipLengthEncodedString(data[pos:])
		if err != nil {
			return nil, err
		}
		pos += n

		// Filler [uint8]
		pos++

		// Charset [charset, collation uint8]
		columns[i].charSet = data[pos]
		pos += 2

		// Length [uint32]
		columns[i].length = binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4

		// Field type [uint8]
		columns[i].fieldType = fieldType(data[pos])
		pos++

		// Flags [uint16]
		columns[i].flags = fieldFlag(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2

		// Decimals [uint8]
		columns[i].decimals = data[pos]
		//pos++

		// Default value [len coded binary]
		//if pos < len(data) {
		//	defaultVal, _, err = bytesToLengthCodedBinary(data[pos:])
		//}
	}
}

func (rows *textRows) ResultSetRecordCount() int64 {
	mc := rows.stmt.mc
	data, err := mc.buf.takeBuffer(25 + 4*3)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(rows.stmt.id))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(rows.cursorId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(1))
	pos += 4
	datain, err := mc.requestServer1(RESULT_SET_GET_RECORD_COUNT, data[:pos])
	if err != nil || datain == nil || len(datain) < 9 || datain[0] != 1 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(datain[1:9]))
}

// Read Packets as Field Packets until EOF-Packet or an Error appears
// http://dev.cloudwave.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
func (rows *textRows) readRow(dest []driver.Value) error {
	mc := rows.stmt.mc

	if rows.rs.done {
		return io.EOF
	}
	dataout, err := mc.buf.takeBuffer(25 + 4*3)

	pos := 25
	binary.BigEndian.PutUint32(dataout[pos:], uint32(rows.stmt.id))
	//	binary.BigEndian.PutUint32(dataout[pos:], uint32(rows.stmt.id))
	pos += 4
	binary.BigEndian.PutUint32(dataout[pos:], uint32(rows.cursorId))
	//	binary.BigEndian.PutUint32(dataout[pos:], uint32(rows.cursorId))
	pos += 4
	binary.BigEndian.PutUint32(dataout[pos:], uint32(1))
	pos += 4
	datain, err := mc.requestServer1(RESULT_SET_QUERY_NEXT, dataout[:pos])
	if err != nil {
		return err
	}
	if datain == nil {
		return io.EOF
	}

	if datain[0] != 1 {
		// server_status [2 bytes]
		//		rows.mc.status = readStatus(data[3:])
		rows.rs.done = true
		if !rows.HasNextResultSet() {
			rows.stmt.mc = nil
		}
		return io.EOF
	}

	// RowSet Packet
	var n int
	var tp byte
	// Read bytes and convert to string
	if datain[1] == 0 {
		rows.stmt.mc.status = statusNoIndexUsed
		rows.rs.done = true
		return io.EOF
	}
	pos = 2
	size := int(binary.BigEndian.Uint32(datain[pos:]))
	if size <= 0 {
		return io.EOF
	}
	pos += 4
	i := 0
	start := 1
	for {
		var scale int
		if i >= len(dest) {
			break
		}
		dest[i], tp, scale, n, err = rows.readObject(datain[pos:])
		if err != nil {
			break
		}
		if start != 1 {
			i++
		}
		start = 0
		if tp != CLOUD_TYPE_ZONE_AUTO_SEQUENCE {
			//i++
		}

		pos += n
		scale = scale
		/*
		   		// Parse time field
		   //		rows.rs.columns[i].fieldType = tp
		   		switch tp {
		   		case CLOUD_TYPE_DATE,
		   			CLOUD_TYPE_TIME,
		   			CLOUD_TYPE_TIMESTAMP:
		   			if dest[i], err = parseDateTime(dest[i].([]byte), mc.cfg.Loc); err != nil {
		   				return err
		   			}
		   		}
		*/
	}
	return nil
}

// Reads Packets until EOF-Packet or an Error appears. Returns count of Packets read
func (mc *cwConn) readUntilEOF() error {
	return nil //weiping ?????
	for {
		data, err := mc.readPacket()
		if err != nil {
			return err
		}

		switch data[0] {
		case iERR:
			return mc.handleErrorPacket(data)
		case iEOF:
			if len(data) == 5 {
				mc.status = readStatus(data[3:])
			}
			return nil
		}
	}
}

/******************************************************************************
*                           Prepared Statements                               *
******************************************************************************/

// Prepare Result Packets
// http://dev.cloudwave.com/doc/internals/en/com-stmt-prepare-response.html
func (stmt *cwStmt) readPrepareResultPacket() (int, error) {
	data, err := stmt.mc.readPacket()
	if err != nil {
		return 0, err
	}
	if data[0] != iOK {
		return 0, stmt.mc.handleErrorPacket(data)
	}
	stmt.id = binary.BigEndian.Uint32(data[1:])
	stmt.executeSequence = 0
	bindVarCount := int(binary.BigEndian.Uint32(data[5:]))
	stmt.paramCount = bindVarCount
	buf := make([]byte, bindVarCount)
	copy(buf, data[9:9+bindVarCount])
	stmt.paramType = buf
	stmt.cursorId = int32(binary.BigEndian.Uint32(data[9+bindVarCount:]))
	return bindVarCount, nil
}

// Execute Prepared Statement
// http://dev.cloudwave.com/doc/internals/en/com-stmt-execute.html
func (stmt *cwStmt) writeExecutePacket(executeType int, args []driver.Value) error {
	if len(args) != stmt.paramCount {
		return fmt.Errorf(
			"argument count mismatch (got: %d; has: %d)",
			len(args),
			stmt.paramCount,
		)
	}

	const minPktLen = 25 + 20
	mc := stmt.mc

	// Determine threshold dynamically to avoid packet size shortage.
	longDataSize := mc.maxAllowedPacket / (stmt.paramCount + 1)
	if longDataSize < 64 {
		longDataSize = 64
	}

	// Reset packet-sequence
	mc.sequence = 0

	var data []byte
	var err error

	if len(args) == 0 {
		data, err = mc.buf.takeBuffer(minPktLen)
	} else {
		data, err = mc.buf.takeCompleteBuffer()
		// In this case the len(data) == cap(data) which is used to optimise the flow below.
	}
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return err
	}

	pos := 25
	// statement_id [4 bytes]
	binary.BigEndian.PutUint32(data[pos:], uint32(stmt.id))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(stmt.executeSequence))
	stmt.executeSequence++
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(stmt.cursorId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(executeType))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(stmt.paramCount))
	pos += 4

	if len(args) > 0 {
		for i, arg := range args {
			binary.BigEndian.PutUint32(data[pos:], uint32(i+1))
			pos += 4
			n, err := stmt.writeObject(arg, stmt.paramType[i], 10, data[pos:])
			if err != nil {
				return err
			}
			pos += n
		}
	}
	mc.setCommandPacket(EXECUTE_PREPARED_STATEMENT, pos, data[0:25])

	return mc.writePacket(data[0:pos])
}

// Execute Prepared Statement
// http://dev.cloudwave.com/doc/internals/en/com-stmt-execute.html
func (stmt *cwStmt) writeTxBatchExecutePacket(args []driver.Value) error {
	if len(args) != stmt.paramCount {
		return fmt.Errorf(
			"argument count mismatch (got: %d; has: %d)",
			len(args),
			stmt.paramCount,
		)
	}

	const minPktLen = 25 + 20
	mc := stmt.mc

	// Determine threshold dynamically to avoid packet size shortage.
	longDataSize := mc.maxAllowedPacket / (stmt.paramCount + 1)
	if longDataSize < 64 {
		longDataSize = 64
	}

	// Reset packet-sequence
	mc.sequence = 0

	var data []byte
	var err error

	if len(args) == 0 {
		data, err = mc.buf.takeBuffer(minPktLen)
	} else {
		data, err = mc.buf.takeCompleteBuffer()
		// In this case the len(data) == cap(data) which is used to optimise the flow below.
	}
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return err
	}

	pos := 25
	// statement_id [4 bytes]
	binary.BigEndian.PutUint32(data[pos:], uint32(stmt.id))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(stmt.executeSequence))
	stmt.executeSequence++
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(stmt.cursorId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(stmt.paramCount))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(1))
	pos += 4

	if len(args) > 0 {
		for i, arg := range args {
			n, err := stmt.writeObject(arg, stmt.paramType[i], 10, data[pos:])
			if err != nil {
				return err
			}
			pos += n
		}
	}
	mc.setCommandPacket(EXECUTE_BATCH_PREPARED, pos, data[0:25])

	return mc.writePacket(data[0:pos])
}

func (mc *cwConn) discardResults() error {
	for mc.status&statusMoreResultsExists != 0 {
		resLen, err := mc.readResultSetHeaderPacket()
		if err != nil {
			return err
		}
		if resLen > 0 {
			// columns
			if err := mc.readUntilEOF(); err != nil {
				return err
			}
			// rows
			if err := mc.readUntilEOF(); err != nil {
				return err
			}
		}
	}
	return nil
}

// http://dev.cloudwave.com/doc/internals/en/binary-protocol-resultset-row.html
func (rows *binaryRows) readRow(dest []driver.Value) error {
	data, err := rows.stmt.mc.readPacket()
	if err != nil {
		return err
	}

	// packet indicator [1 byte]
	if data[0] != iOK {
		// EOF Packet
		if data[0] == iEOF && len(data) == 5 {
			rows.stmt.mc.status = readStatus(data[3:])
			rows.rs.done = true
			if !rows.HasNextResultSet() {
				rows.stmt.mc = nil
			}
			return io.EOF
		}
		mc := rows.stmt.mc
		rows.stmt.mc = nil

		// Error otherwise
		return mc.handleErrorPacket(data)
	}

	// NULL-bitmap,  [(column-count + 7 + 2) / 8 bytes]
	pos := 1 + (len(dest)+7+2)>>3
	nullMask := data[1:pos]

	for i := range dest {
		// Field is NULL
		// (byte >> bit-pos) % 2 == 1
		if ((nullMask[(i+2)>>3] >> uint((i+2)&7)) & 1) == 1 {
			dest[i] = nil
			continue
		}

		// Convert to byte-coded string
		switch rows.rs.columns[i].fieldType {
		case CLOUD_TYPE_NULL:
			dest[i] = nil
			continue

		// Numeric Types
		case CLOUD_TYPE_INTEGER, CLOUD_TYPE_TINY_INTEGER:
			dest[i] = int32(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos++
			continue

		case CLOUD_TYPE_BIG_INTEGER: //?????
			dest[i] = int64(int32(binary.BigEndian.Uint32(data[pos : pos+4])))
			pos += 4
			continue

		case CLOUD_TYPE_SMALL_INTEGER, CLOUD_TYPE_LONG:
			dest[i] = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			continue

		case CLOUD_TYPE_FLOAT:
			dest[i] = math.Float32frombits(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			continue

		case CLOUD_TYPE_DOUBLE:
			dest[i] = math.Float64frombits(binary.BigEndian.Uint64(data[pos : pos+8]))
			pos += 8
			continue

		// Length coded Binary Strings
		case CLOUD_TYPE_TINY_DECIMAL, CLOUD_TYPE_SMALL_DECIMAL, CLOUD_TYPE_BIG_DECIMAL,
			CLOUD_TYPE_VARCHAR, CLOUD_TYPE_BYTE,
			CLOUD_TYPE_BLOB:
			var isNull bool
			var n int
			dest[i], isNull, n, err = ReadLengthEncodedString(data[pos:])
			pos += n
			if err == nil {
				if !isNull {
					continue
				} else {
					dest[i] = nil
					continue
				}
			}
			return err

		case
			CLOUD_TYPE_DATE,      // Date YYYY-MM-DD
			CLOUD_TYPE_TIME,      // Time [-][H]HH:MM:SS[.fractal]
			CLOUD_TYPE_TIMESTAMP: // Timestamp YYYY-MM-DD HH:MM:SS[.fractal]

			num, isNull, n := readLengthEncodedInteger(data[pos:])
			pos += n

			switch {
			case isNull:
				dest[i] = nil
				continue
			case rows.rs.columns[i].fieldType == CLOUD_TYPE_TIME:
				// database/sql does not support an equivalent to TIME, return a string
				var dstlen uint8
				switch decimals := rows.rs.columns[i].decimals; decimals {
				case 0x00, 0x1f:
					dstlen = 8
				case 1, 2, 3, 4, 5, 6:
					dstlen = 8 + 1 + decimals
				default:
					return fmt.Errorf(
						"protocol error, illegal decimals value %d",
						rows.rs.columns[i].decimals,
					)
				}
				dest[i], err = formatBinaryTime(data[pos:pos+int(num)], dstlen)
			case rows.stmt.mc.parseTime:
				dest[i], err = parseBinaryDateTime(num, data[pos:], rows.stmt.mc.cfg.Loc)
			default:
				var dstlen uint8
				if rows.rs.columns[i].fieldType == CLOUD_TYPE_DATE {
					dstlen = 10
				} else {
					switch decimals := rows.rs.columns[i].decimals; decimals {
					case 0x00, 0x1f:
						dstlen = 19
					case 1, 2, 3, 4, 5, 6:
						dstlen = 19 + 1 + decimals
					default:
						return fmt.Errorf(
							"protocol error, illegal decimals value %d",
							rows.rs.columns[i].decimals,
						)
					}
				}
				dest[i], err = formatBinaryDateTime(data[pos:pos+int(num)], dstlen)
			}

			if err == nil {
				pos += int(num)
				continue
			} else {
				return err
			}

		// Please report if this happens!
		default:
			return fmt.Errorf("unknown field type %d", rows.rs.columns[i].fieldType)
		}
	}

	return nil
}
