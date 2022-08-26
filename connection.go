// Go CloudWave Driver - A CloudWave-Driver for Go's database/sql package
//
// Copyright 2012 The Go-CloudWave-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package cloudwave

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type cwConn struct {
	buf              buffer
	netConn          net.Conn
	rawConn          net.Conn // underlying connection when netConn is TLS connection.
	affectedRows     uint64
	insertId         uint64
	cfg              *Config
	maxAllowedPacket int
	maxWriteSize     int
	writeTimeout     time.Duration
	flags            clientFlag
	status           statusFlag
	sequence         uint8	// sequence for read packet
	parseTime        bool
	reset            bool // set when the Go SQL package calls ResetSession

	// for context support (Go 1.8+)
	watching bool
	watcher  chan<- context.Context
	closech  chan struct{}
	finished chan<- struct{}
	canceled atomicError // set non-nil if conn is canceled
	closed   atomicBool  // set when conn is closed, before closech is closed

	txBatchFlag      bool

//add vars for cloudwave
	sessionTime		uint64
	sessionSequence	uint64
	sessionToken	uint64

	execType	byte
}

const DATAAREALEN	= 32
const DataAvailableTime = 3  //Seconds
type stru_dataArea struct {
	dataLock sync.Mutex
	dataPoint int
	dataUnusable [DATAAREALEN]bool
	dataTimestamp [DATAAREALEN]time.Time
	data [DATAAREALEN][]byte
}
var dataArea stru_dataArea

func pushData(b []byte) int {
	dataArea.dataLock.Lock()
	defer dataArea.dataLock.Unlock()
	tm := time.Now()
	stratpoint := dataArea.dataPoint
	for {
		dataArea.dataPoint++
		if dataArea.dataPoint >= DATAAREALEN {
			dataArea.dataPoint = 0
		}
		if dataArea.dataUnusable[dataArea.dataPoint] == false {
			break
		}
		if dataArea.dataPoint == stratpoint {
			break
		}
	}
	if dataArea.dataPoint == stratpoint {
		for {
			dataArea.dataPoint++
			if dataArea.dataPoint >= DATAAREALEN {
				dataArea.dataPoint = 0
			}
			t := tm.Sub(dataArea.dataTimestamp[dataArea.dataPoint]).Seconds()
			if t >= DataAvailableTime {
				break
			}
			if dataArea.dataPoint == stratpoint {
				break
			}
		}
	}
//	if dataArea[dataAreaPoint] == nil || len(dataArea[dataAreaPoint]) < len(b) {
		dataArea.data[dataArea.dataPoint] = make([]byte, len(b))
//	}
	copy(dataArea.data[dataArea.dataPoint], b)
	dataArea.dataUnusable[dataArea.dataPoint] = true
	dataArea.dataTimestamp[dataArea.dataPoint] = tm

	i := dataArea.dataPoint
	return i
}

func PullData(i int) []byte {
	var buf []byte
	dataArea.dataLock.Lock()
	defer dataArea.dataLock.Unlock()
	if i >= 0 && i < DATAAREALEN {
		buf = make([]byte, len(dataArea.data[i]))
		copy(buf, dataArea.data[i])
		dataArea.dataUnusable[dataArea.dataPoint] = false
		return buf
	}
	return nil
}

// Handles parameters set in DSN after the connection is established
func (mc *cwConn) handleParams() (err error) {
	var cmdSet strings.Builder
	for param, val := range mc.cfg.Params {
		switch param {
		// Charset: character_set_connection, character_set_client, character_set_results
		case "charset":
			charsets := strings.Split(val, ",")
			for i := range charsets {
				// ignore errors here - a charset may not exist
				err := mc.exec("SET NAMES " + charsets[i])
				if err == nil {
					break
				}
			}
			if err != nil {
				return
			}

		// Other system vars accumulated in a single SET command
		default:
			if cmdSet.Len() == 0 {
				// Heuristic: 29 chars for each other key=value to reduce reallocations
				cmdSet.Grow(4 + len(param) + 1 + len(val) + 30*(len(mc.cfg.Params)-1))
				cmdSet.WriteString("SET ")
			} else {
				cmdSet.WriteByte(',')
			}
			cmdSet.WriteString(param)
			cmdSet.WriteByte('=')
			cmdSet.WriteString(val)
		}
	}

	if cmdSet.Len() > 0 {
		err := mc.exec(cmdSet.String())
		if err != nil {
			return err
		}
	}

	return
}

func (mc *cwConn) markBadConn(err error) error {
	if mc == nil {
		return err
	}
	if err != errBadConnNoWrite {
		return err
	}
	return driver.ErrBadConn
}

func (mc *cwConn) Begin() (driver.Tx, error) {
	return mc.begin(false)
}

func (mc *cwConn) begin(readOnly bool) (driver.Tx, error) {
	if mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	return &cwTx{mc}, nil
}

func (mc *cwConn) Close() (err error) {
	// Makes Close idempotent
	if !mc.closed.IsSet() {
		err = mc.writeCommandPacket(B_REQ_CLOSE_CONNECTION)
	}

	mc.cleanup()

	return
}

// Closes the network connection and unsets internal variables. Do not call this
// function after successfully authentication, call Close instead. This function
// is called before auth or on auth failure because MySQL will have already
// closed the network connection.
func (mc *cwConn) cleanup() {
	if !mc.closed.TrySet(true) {
		return
	}

	// Makes cleanup idempotent
	close(mc.closech)
	if mc.netConn == nil {
		return
	}
	if err := mc.netConn.Close(); err != nil {
		errLog.Print(err)
	}
}

func (mc *cwConn) error() error {
	if mc.closed.IsSet() {
		if err := mc.canceled.Value(); err != nil {
			return err
		}
		return ErrInvalidConn
	}
	return nil
}

func (mc *cwConn) createStatement() (*cwStmt, error) {
	stmt := &cwStmt{
		mc: mc,
		stmtType: CONNECTION_CREATE_STATEMENT,
	}
	resultdata, err := mc.requestServer(CONNECTION_CREATE_STATEMENT)
	if err != nil {
		return nil, err
	}

	resultdatalen := len(resultdata)
	if resultdatalen > 0 {
		if resultdata[0] == iOK {
			if resultdatalen >= 5 {
				stmt.id = binary.BigEndian.Uint32(resultdata[1:])
				stmt.executeSequence = 0;
				stmt.paramCount = 0
				stmt.paramType = nil
				stmt.cursorId = 0
				return stmt, nil
			}
		}
	}
	err = errors.New("CONNECTION_CREATE_STATEMENT error")
	return nil, err
}

func (mc *cwConn) setAutoCommit(autoCommit bool) error {
	data, err := mc.buf.takeBuffer(26)
	if err != nil {
		return ErrBusyBuffer
	}
	if autoCommit {
		data[25] = 1
	} else {
		data[25] = 0
	}
	mc.setCommandPacket(CONNECTION_SET_AUTO_COMMIT, 26, data[0:25])
	err = mc.writePacket(data[0:26])
	if err == nil {
		_, err = mc.readResultOK()
	}
	return err
}

func (mc *cwConn) Prepare(query string) (driver.Stmt, error) {
	mc.execType = whichExecute(query)
	if mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// Send command

	mc.sequence = 0

	pktLen := 25 + 4 + len(query) + 4
	data, err := mc.buf.takeBuffer(pktLen)
	if err != nil {
		// cannot take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return nil, errBadConnNoWrite
	}
	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(len(query)))
	pos += 4
	pos += copy(data[pos:], query)
	binary.BigEndian.PutUint32(data[pos:], uint32(2))	//NO_GENERATED_KEYS=2
	pos += 4
	mc.setCommandPacket(CONNECTION_PREPARED_STATEMENT, pos, data[0:25])

	err = mc.writePacket(data[0:pos])
	if err != nil {
		return nil, driver.ErrBadConn
	}

	stmt := &cwStmt{
		mc: mc,
		stmtType: CONNECTION_PREPARED_STATEMENT,
	}

	// Read Result
	columnCount, err := stmt.readPrepareResultPacket()
	if err == nil {
		if stmt.paramCount > 0 {
			if err = mc.readUntilEOF(); err != nil {
				return nil, err
			}
		}

		if columnCount > 0 {
			err = mc.readUntilEOF()
		}
	}

	return stmt, err
}

func (mc *cwConn) interpolateParams(query string, args []driver.Value) (string, error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}

	buf, err := mc.buf.takeCompleteBuffer()
	if err != nil {
		// can not take the buffer. Something must be wrong with the connection
		errLog.Print(err)
		return "", ErrInvalidConn
	}
	buf = buf[:0]
	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case uint64:
			// Handle uint64 explicitly because our custom ConvertValue emits unsigned values
			buf = strconv.AppendUint(buf, v, 10)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				buf = append(buf, '\'')
				buf, err = appendDateTime(buf, v.In(mc.cfg.Loc))
				if err != nil {
					return "", err
				}
				buf = append(buf, '\'')
			}
		case json.RawMessage:
			buf = append(buf, '\'')
			if mc.status&statusNoBackslashEscapes == 0 {
				buf = escapeBytesBackslash(buf, v)
			} else {
				buf = escapeBytesQuotes(buf, v)
			}
			buf = append(buf, '\'')
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				buf = append(buf, "_binary'"...)
				if mc.status&statusNoBackslashEscapes == 0 {
					buf = escapeBytesBackslash(buf, v)
				} else {
					buf = escapeBytesQuotes(buf, v)
				}
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			if mc.status&statusNoBackslashEscapes == 0 {
				buf = escapeStringBackslash(buf, v)
			} else {
				buf = escapeStringQuotes(buf, v)
			}
			buf = append(buf, '\'')
		default:
			return "", driver.ErrSkip
		}

		if len(buf)+4 > mc.maxAllowedPacket {
			return "", driver.ErrSkip
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return string(buf), nil
}

func (mc *cwConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	mc.execType = whichExecute(query)
	if mc.execType == CLOUDWAVE_SELFUSEDRIVE {
		var buf []byte
		var err error
		byteflag := false
		cmd := -10000
		if len(args) > 0 {
			switch v := args[0].(type) {
			case int64:
				cmd = int(v)
			}
		}
		if cmd > -10000 {
			if cmd >= 10000 {
				byteflag = true
				cmd -= 10000
			}
			if len(args) == 1 {
				err = mc.writeCommandPacket(cmd)
			} else {
				var data []byte
				data, err = mc.buf.takeCompleteBuffer()
				if err != nil {
					return nil, ErrBusyBuffer
				}
				pos := 25
				for i := 1; i < len(args); i++ {
					switch v := args[i].(type) {
					case bool:
						if v {
							data[pos] = 1
						} else {
							data[pos] = 0
						}
						pos++
					case float64:
						binary.BigEndian.PutUint32(data[pos:pos+4], uint32(int32(v)))
						pos += 4
					case int64:
						binary.BigEndian.PutUint64(data[pos:pos+8], uint64(v))
						pos += 8
					case []byte:
						if byteflag {
							pos += copy(data[pos:], v)
						} else {
							pos++
							if v == nil {
								data[pos-1] = 1
								continue
							} else {
								data[pos-1] = 0
							}
							n := copy(data[pos+4:], v)
							binary.BigEndian.PutUint32(data[pos:pos+4], uint32(n))
							pos += (4 + n)
						}
					case string:
						n := copy(data[pos+4:], v)
						binary.BigEndian.PutUint32(data[pos:pos+4], uint32(n))
						pos += (4 + n)
					case json.RawMessage:
						js := v
						var ss []string
						err = json.Unmarshal(js, &ss)
						if len(ss) <= 0 {
							data[pos] = 1
							pos++
						} else {
							data[pos] = 0
							pos++
							for i := 0; i < len(ss); i++ {
								data[pos] = 0
								pos++
								n := copy(data[pos+4:], ss[i])
								binary.BigEndian.PutUint32(data[pos:pos+4], uint32(n))
								pos += (n + 4)
							}
						}
					}
				}
				err = mc.setCommandPacket(cmd, pos, data[0:25])
				if err == nil {
					err = mc.writePacket(data[0:pos])
				}
			}
			if err == nil {
				buf, err = mc.readResultOK()
			}
			if err == nil {
				i := int64(pushData(buf))
				return &cwResult{
					affectedRows: i,
					insertId:     0,
				}, nil
			}
		} else {
			err = errors.New("command code is error")
		}
		return nil, err
	}
	if mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		if !mc.cfg.InterpolateParams {
//			return nil, driver.ErrSkip
		}
		// try to interpolate the parameters to save extra roundtrips for preparing and closing a statement
		prepared, err := mc.interpolateParams(query, args)
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	mc.affectedRows = 0
	mc.insertId = 0

	err := mc.exec(query)
	if err == nil {
		return &cwResult{
			affectedRows: int64(mc.affectedRows),
			insertId:     int64(mc.insertId),
		}, err
	}
	return nil, mc.markBadConn(err)
}

// Internal function to execute commands
func (mc *cwConn) exec(query string) error {
	stmt, err := mc.createStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Send command
	if err = stmt.writeCommandPacketStr(mc.execType, query); err != nil {
		return mc.markBadConn(err)
	}

	// Read Result
	resLen, _, err := stmt.readResultSetHeaderPacket2()
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

	return mc.discardResults()
}

func (mc *cwConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return mc.query(query, args)
}

type ssss struct {
	ss [10]string
}
func (mc *cwConn) query(query string, args []driver.Value) (*textRows, error) {
	var err error
	var cmd int
	var stmt *cwStmt

	if mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}

	mc.execType = whichExecute(query)
	byteflag := false
	if mc.execType == CLOUDWAVE_SELFUSEDRIVE {
		cmd = -10000
		if len(args) > 0 {
			switch v := args[0].(type) {
			case int64:
				cmd = int(v)
			}
		}
		if cmd <= -10000 {
			return nil, errors.New("command code is error")
		}
		if cmd >= 10000 {
			byteflag = true
			cmd -= 10000
		}
	} else {
		if len(args) != 0 {
			if !mc.cfg.InterpolateParams {
//				return nil, driver.ErrSkip
			}
			// try client-side prepare to reduce roundtrip
			prepared, err := mc.interpolateParams(query, args)
			if err != nil {
				return nil, err
			}
			query = prepared
		}
	}
	stmt, err = mc.createStatement()
	if err != nil {
		return nil, err
	}

	if mc.execType == CLOUDWAVE_SELFUSEDRIVE {
		if len(args) == 1 {
			err = stmt.mc.writeCommandPacket(cmd)
		} else {
			var data []byte
			data, err = mc.buf.takeCompleteBuffer()
			if err != nil {
				return nil, ErrBusyBuffer
			}
			pos := 25
			for i := 1; i < len(args); i++ {
				switch v := args[i].(type) {
				case bool:
					if v {
						data[pos] = 1
					} else {
						data[pos] = 0
					}
					pos++
				case float64:
					binary.BigEndian.PutUint32(data[pos:pos+4], uint32(int32(v)))
					pos += 4
				case int64:
					binary.BigEndian.PutUint64(data[pos:pos+8], uint64(v))
					pos += 8
				case []byte:
					if byteflag {
						pos += copy(data[pos:], v)
					} else {
						pos++
						if v == nil {
							data[pos-1] = 1
							continue
						} else {
							data[pos-1] = 0
						}
						n := copy(data[pos+4:], v)
						binary.BigEndian.PutUint32(data[pos:pos+4], uint32(n))
						pos += (4 + n)
					}
				case string:
					n := copy(data[pos+4:], v)
					binary.BigEndian.PutUint32(data[pos:pos+4], uint32(n))
					pos += (4 + n)
				case json.RawMessage:
					js := v
					var ss []string
					err = json.Unmarshal(js, &ss)
					if len(ss) <= 0 {
						data[pos] = 1
						pos++
					} else {
						data[pos] = 0
						pos++
						for i := 0; i < len(ss); i++ {
							data[pos] = 0
							pos++
							n := copy(data[pos+4:], ss[i])
							binary.BigEndian.PutUint32(data[pos:pos+4], uint32(n))
							pos += (n + 4)
						}
					}
				}
			}
			err = mc.setCommandPacket(cmd, pos, data[0:25])
			if err == nil {
				err = mc.writePacket(data[0:pos])
			}
		}
	} else {
		// Send command
		err = stmt.writeCommandPacketStr(CLOUDWAVE_EXECUTE_QUERY, query)
	}
	if err != nil {
		return nil, mc.markBadConn(err)
	}
	// Read Result
	var resLen int

	resLen, rows, err := stmt.readResultSetHeaderPacket2()
	if err != nil {
		return nil, mc.markBadConn(err)
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
	if mc.execType == CLOUDWAVE_SELFUSEDRIVE {
//		rows.resultCount = rows.ResultSetRecordCount()
	}
	return rows, nil
}

// finish is called when the query has canceled.
func (mc *cwConn) cancel(err error) {
	mc.canceled.Set(err)
	mc.cleanup()
}

// finish is called when the query has succeeded.
func (mc *cwConn) finish() {
	if !mc.watching || mc.finished == nil {
		return
	}
	select {
	case mc.finished <- struct{}{}:
		mc.watching = false
	case <-mc.closech:
	}
}

// Ping implements driver.Pinger interface
func (mc *cwConn) Ping(ctx context.Context) (err error) {
	if mc.closed.IsSet() {
		errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}

	if err = mc.watchCancel(ctx); err != nil {
		return
	}
	defer mc.finish()

	if err = mc.writeCommandPacket(GET_SERVER_VERSION); err != nil {
		return mc.markBadConn(err)
	}

	_, err = mc.readResultOK()
	return err
}

// BeginTx implements driver.ConnBeginTx interface
func (mc *cwConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if mc.closed.IsSet() {
		return nil, driver.ErrBadConn
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	mc.txBatchFlag = true
	mc.setAutoCommit(false)


	defer mc.finish()

	if sql.IsolationLevel(opts.Isolation) != sql.LevelDefault {
		level := sql.IsolationLevel(opts.Isolation)

		data, err := mc.buf.takeBuffer(29)
		if err != nil {
			return nil, ErrBusyBuffer
		}
		binary.BigEndian.PutUint32(data[25:], uint32(level))
		mc.setCommandPacket(SET_TRANSACTION_ISOLATION, 29, data[0:25])
		err = mc.writePacket(data[0:29])
		if err == nil {
			_, err = mc.readResultOK()
			if err != nil {
				return nil, err
			}
		}
	}

	return mc.begin(opts.ReadOnly)
}

func (mc *cwConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	rows, err := mc.query(query, dargs)
	if err != nil {
		mc.finish()
		return nil, err
	}
	rows.finish = mc.finish
	return rows, err
}

func (mc *cwConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer mc.finish()

	return mc.Exec(query, dargs)
}

func (mc *cwConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	stmt, err := mc.Prepare(query)
	mc.finish()
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		stmt.Close()
		return nil, ctx.Err()
	}
	return stmt, nil
}

func (stmt *cwStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := stmt.mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	rows, err := stmt.query(dargs)
	if err != nil {
		stmt.mc.finish()
		return nil, err
	}
	rows.finish = stmt.mc.finish
	return rows, err
}

func (stmt *cwStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}

	if err := stmt.mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer stmt.mc.finish()

	return stmt.Exec(dargs)
}

func (mc *cwConn) watchCancel(ctx context.Context) error {
	if mc.watching {
		// Reach here if canceled,
		// so the connection is already invalid
		mc.cleanup()
		return nil
	}
	// When ctx is already cancelled, don't watch it.
	if err := ctx.Err(); err != nil {
		return err
	}
	// When ctx is not cancellable, don't watch it.
	if ctx.Done() == nil {
		return nil
	}
	// When watcher is not alive, can't watch it.
	if mc.watcher == nil {
		return nil
	}

	mc.watching = true
	mc.watcher <- ctx
	return nil
}

func (mc *cwConn) startWatcher() {
	watcher := make(chan context.Context, 1)
	mc.watcher = watcher
	finished := make(chan struct{})
	mc.finished = finished
	go func() {
		for {
			var ctx context.Context
			select {
			case ctx = <-watcher:
			case <-mc.closech:
				return
			}

			select {
			case <-ctx.Done():
				mc.cancel(ctx.Err())
			case <-finished:
			case <-mc.closech:
				return
			}
		}
	}()
}

func (mc *cwConn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = converter{}.ConvertValue(nv.Value)
	return
}

// ResetSession implements driver.SessionResetter.
// (From Go 1.10)
func (mc *cwConn) ResetSession(ctx context.Context) error {
	if mc.closed.IsSet() {
		return driver.ErrBadConn
	}
	mc.reset = true
	return nil
}

// IsValid implements driver.Validator interface
// (From Go 1.15)
func (mc *cwConn) IsValid() bool {
	return !mc.closed.IsSet()
}
