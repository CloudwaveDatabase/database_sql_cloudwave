// Go CloudWave Driver - A CloudWave-Driver for Go's database/sql package
//
// Copyright 2012 The Go-CloudWave-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package cloudwave

import (
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	//	"math/big"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Registry for custom tls.Configs
var (
	tlsConfigLock     sync.RWMutex
	tlsConfigRegistry map[string]*tls.Config
)

// RegisterTLSConfig registers a custom tls.Config to be used with sql.Open.
// Use the key as a value in the DSN where tls=value.
//
// Note: The provided tls.Config is exclusively owned by the driver after
// registering it.
//
//  rootCertPool := x509.NewCertPool()
//  pem, err := ioutil.ReadFile("/path/ca-cert.pem")
//  if err != nil {
//      log.Fatal(err)
//  }
//  if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
//      log.Fatal("Failed to append PEM.")
//  }
//  clientCert := make([]tls.Certificate, 0, 1)
//  certs, err := tls.LoadX509KeyPair("/path/client-cert.pem", "/path/client-key.pem")
//  if err != nil {
//      log.Fatal(err)
//  }
//  clientCert = append(clientCert, certs)
//  cloudwave.RegisterTLSConfig("custom", &tls.Config{
//      RootCAs: rootCertPool,
//      Certificates: clientCert,
//  })
//  db, err := sql.Open("cloudwave", "user@tcp(localhost:3306)/test?tls=custom")
//
func RegisterTLSConfig(key string, config *tls.Config) error {
	if _, isBool := readBool(key); isBool || strings.ToLower(key) == "skip-verify" || strings.ToLower(key) == "preferred" {
		return fmt.Errorf("key '%s' is reserved", key)
	}

	tlsConfigLock.Lock()
	if tlsConfigRegistry == nil {
		tlsConfigRegistry = make(map[string]*tls.Config)
	}

	tlsConfigRegistry[key] = config
	tlsConfigLock.Unlock()
	return nil
}

// DeregisterTLSConfig removes the tls.Config associated with key.
func DeregisterTLSConfig(key string) {
	tlsConfigLock.Lock()
	if tlsConfigRegistry != nil {
		delete(tlsConfigRegistry, key)
	}
	tlsConfigLock.Unlock()
}

func getTLSConfigClone(key string) (config *tls.Config) {
	tlsConfigLock.RLock()
	if v, ok := tlsConfigRegistry[key]; ok {
		config = v.Clone()
	}
	tlsConfigLock.RUnlock()
	return
}

// Returns the bool value of the input.
// The 2nd return value indicates if the input was a valid bool value
func readBool(input string) (value bool, valid bool) {
	switch input {
	case "1", "true", "TRUE", "True":
		return true, true
	case "0", "false", "FALSE", "False":
		return false, true
	}

	// Not a valid bool value
	return
}

/******************************************************************************
*                           Time related utils                                *
******************************************************************************/

func parseDateTime(b []byte, loc *time.Location) (time.Time, error) {
	const base = "0000-00-00 00:00:00.000000"
	switch len(b) {
	case 10, 19, 21, 22, 23, 24, 25, 26: // up to "YYYY-MM-DD HH:MM:SS.MMMMMM"
		if string(b) == base[:len(b)] {
			return time.Time{}, nil
		}

		year, err := parseByteYear(b)
		if err != nil {
			return time.Time{}, err
		}
		if year <= 0 {
			year = 1
		}

		if b[4] != '-' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[4])
		}

		m, err := parseByte2Digits(b[5], b[6])
		if err != nil {
			return time.Time{}, err
		}
		if m <= 0 {
			m = 1
		}
		month := time.Month(m)

		if b[7] != '-' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[7])
		}

		day, err := parseByte2Digits(b[8], b[9])
		if err != nil {
			return time.Time{}, err
		}
		if day <= 0 {
			day = 1
		}
		if len(b) == 10 {
			return time.Date(year, month, day, 0, 0, 0, 0, loc), nil
		}

		if b[10] != ' ' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[10])
		}

		hour, err := parseByte2Digits(b[11], b[12])
		if err != nil {
			return time.Time{}, err
		}
		if b[13] != ':' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[13])
		}

		min, err := parseByte2Digits(b[14], b[15])
		if err != nil {
			return time.Time{}, err
		}
		if b[16] != ':' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[16])
		}

		sec, err := parseByte2Digits(b[17], b[18])
		if err != nil {
			return time.Time{}, err
		}
		if len(b) == 19 {
			return time.Date(year, month, day, hour, min, sec, 0, loc), nil
		}

		if b[19] != '.' {
			return time.Time{}, fmt.Errorf("bad value for field: `%c`", b[19])
		}
		nsec, err := parseByteNanoSec(b[20:])
		if err != nil {
			return time.Time{}, err
		}
		return time.Date(year, month, day, hour, min, sec, nsec, loc), nil
	default:
		return time.Time{}, fmt.Errorf("invalid time bytes: %s", b)
	}
}

func parseByteYear(b []byte) (int, error) {
	year, n := 0, 1000
	for i := 0; i < 4; i++ {
		v, err := bToi(b[i])
		if err != nil {
			return 0, err
		}
		year += v * n
		n /= 10
	}
	return year, nil
}

func parseByte2Digits(b1, b2 byte) (int, error) {
	d1, err := bToi(b1)
	if err != nil {
		return 0, err
	}
	d2, err := bToi(b2)
	if err != nil {
		return 0, err
	}
	return d1*10 + d2, nil
}

func parseByteNanoSec(b []byte) (int, error) {
	ns, digit := 0, 100000 // max is 6-digits
	for i := 0; i < len(b); i++ {
		v, err := bToi(b[i])
		if err != nil {
			return 0, err
		}
		ns += v * digit
		digit /= 10
	}
	// nanoseconds has 10-digits. (needs to scale digits)
	// 10 - 6 = 4, so we have to multiple 1000.
	return ns * 1000, nil
}

func bToi(b byte) (int, error) {
	if b < '0' || b > '9' {
		return 0, errors.New("not [0-9]")
	}
	return int(b - '0'), nil
}

func parseBinaryDateTime(num uint64, data []byte, loc *time.Location) (driver.Value, error) {
	switch num {
	case 0:
		return time.Time{}, nil
	case 4:
		return time.Date(
			int(binary.LittleEndian.Uint16(data[:2])), // year
			time.Month(data[2]),                       // month
			int(data[3]),                              // day
			0, 0, 0, 0,
			loc,
		), nil
	case 7:
		return time.Date(
			int(binary.LittleEndian.Uint16(data[:2])), // year
			time.Month(data[2]),                       // month
			int(data[3]),                              // day
			int(data[4]),                              // hour
			int(data[5]),                              // minutes
			int(data[6]),                              // seconds
			0,
			loc,
		), nil
	case 11:
		return time.Date(
			int(binary.LittleEndian.Uint16(data[:2])), // year
			time.Month(data[2]),                       // month
			int(data[3]),                              // day
			int(data[4]),                              // hour
			int(data[5]),                              // minutes
			int(data[6]),                              // seconds
			int(binary.LittleEndian.Uint32(data[7:11]))*1000, // nanoseconds
			loc,
		), nil
	}
	return nil, fmt.Errorf("invalid DATETIME packet length %d", num)
}

func appendDateTime(buf []byte, t time.Time) ([]byte, error) {
	year, month, day := t.Date()
	hour, min, sec := t.Clock()
	nsec := t.Nanosecond()

	if year < 1 || year > 9999 {
		return buf, errors.New("year is not in the range [1, 9999]: " + strconv.Itoa(year)) // use errors.New instead of fmt.Errorf to avoid year escape to heap
	}
	year100 := year / 100
	year1 := year % 100

	var localBuf [len("2006-01-02 15:04:05.999999999")]byte // does not escape
	localBuf[0], localBuf[1], localBuf[2], localBuf[3] = digits10[year100], digits01[year100], digits10[year1], digits01[year1]
	localBuf[4] = '-'
	localBuf[5], localBuf[6] = digits10[month], digits01[month]
	localBuf[7] = '-'
	localBuf[8], localBuf[9] = digits10[day], digits01[day]

	if hour == 0 && min == 0 && sec == 0 && nsec == 0 {
		return append(buf, localBuf[:10]...), nil
	}

	localBuf[10] = ' '
	localBuf[11], localBuf[12] = digits10[hour], digits01[hour]
	localBuf[13] = ':'
	localBuf[14], localBuf[15] = digits10[min], digits01[min]
	localBuf[16] = ':'
	localBuf[17], localBuf[18] = digits10[sec], digits01[sec]

	if nsec == 0 {
		return append(buf, localBuf[:19]...), nil
	}
	nsec100000000 := nsec / 100000000
	nsec1000000 := (nsec / 1000000) % 100
	nsec10000 := (nsec / 10000) % 100
	nsec100 := (nsec / 100) % 100
	nsec1 := nsec % 100
	localBuf[19] = '.'

	// milli second
	localBuf[20], localBuf[21], localBuf[22] =
		digits01[nsec100000000], digits10[nsec1000000], digits01[nsec1000000]
	// micro second
	localBuf[23], localBuf[24], localBuf[25] =
		digits10[nsec10000], digits01[nsec10000], digits10[nsec100]
	// nano second
	localBuf[26], localBuf[27], localBuf[28] =
		digits01[nsec100], digits10[nsec1], digits01[nsec1]

	// trim trailing zeros
	n := len(localBuf)
	for n > 0 && localBuf[n-1] == '0' {
		n--
	}

	return append(buf, localBuf[:n]...), nil
}

// zeroDateTime is used in formatBinaryDateTime to avoid an allocation
// if the DATE or DATETIME has the zero value.
// It must never be changed.
// The current behavior depends on database/sql copying the result.
var zeroDateTime = []byte("0000-00-00 00:00:00.000000")

const digits01 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
const digits10 = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"

func appendMicrosecs(dst, src []byte, decimals int) []byte {
	if decimals <= 0 {
		return dst
	}
	if len(src) == 0 {
		return append(dst, ".000000"[:decimals+1]...)
	}

	microsecs := binary.LittleEndian.Uint32(src[:4])
	p1 := byte(microsecs / 10000)
	microsecs -= 10000 * uint32(p1)
	p2 := byte(microsecs / 100)
	microsecs -= 100 * uint32(p2)
	p3 := byte(microsecs)

	switch decimals {
	default:
		return append(dst, '.',
			digits10[p1], digits01[p1],
			digits10[p2], digits01[p2],
			digits10[p3], digits01[p3],
		)
	case 1:
		return append(dst, '.',
			digits10[p1],
		)
	case 2:
		return append(dst, '.',
			digits10[p1], digits01[p1],
		)
	case 3:
		return append(dst, '.',
			digits10[p1], digits01[p1],
			digits10[p2],
		)
	case 4:
		return append(dst, '.',
			digits10[p1], digits01[p1],
			digits10[p2], digits01[p2],
		)
	case 5:
		return append(dst, '.',
			digits10[p1], digits01[p1],
			digits10[p2], digits01[p2],
			digits10[p3],
		)
	}
}

func formatBinaryDateTime(src []byte, length uint8) (driver.Value, error) {
	// length expects the deterministic length of the zero value,
	// negative time and 100+ hours are automatically added if needed
	if len(src) == 0 {
		return zeroDateTime[:length], nil
	}
	var dst []byte      // return value
	var p1, p2, p3 byte // current digit pair

	switch length {
	case 10, 19, 21, 22, 23, 24, 25, 26:
	default:
		t := "DATE"
		if length > 10 {
			t += "TIME"
		}
		return nil, fmt.Errorf("illegal %s length %d", t, length)
	}
	switch len(src) {
	case 4, 7, 11:
	default:
		t := "DATE"
		if length > 10 {
			t += "TIME"
		}
		return nil, fmt.Errorf("illegal %s packet length %d", t, len(src))
	}
	dst = make([]byte, 0, length)
	// start with the date
	year := binary.LittleEndian.Uint16(src[:2])
	pt := year / 100
	p1 = byte(year - 100*uint16(pt))
	p2, p3 = src[2], src[3]
	dst = append(dst,
		digits10[pt], digits01[pt],
		digits10[p1], digits01[p1], '-',
		digits10[p2], digits01[p2], '-',
		digits10[p3], digits01[p3],
	)
	if length == 10 {
		return dst, nil
	}
	if len(src) == 4 {
		return append(dst, zeroDateTime[10:length]...), nil
	}
	dst = append(dst, ' ')
	p1 = src[4] // hour
	src = src[5:]

	// p1 is 2-digit hour, src is after hour
	p2, p3 = src[0], src[1]
	dst = append(dst,
		digits10[p1], digits01[p1], ':',
		digits10[p2], digits01[p2], ':',
		digits10[p3], digits01[p3],
	)
	return appendMicrosecs(dst, src[2:], int(length)-20), nil
}

func formatBinaryTime(src []byte, length uint8) (driver.Value, error) {
	// length expects the deterministic length of the zero value,
	// negative time and 100+ hours are automatically added if needed
	if len(src) == 0 {
		return zeroDateTime[11 : 11+length], nil
	}
	var dst []byte // return value

	switch length {
	case
		8,                      // time (can be up to 10 when negative and 100+ hours)
		10, 11, 12, 13, 14, 15: // time with fractional seconds
	default:
		return nil, fmt.Errorf("illegal TIME length %d", length)
	}
	switch len(src) {
	case 8, 12:
	default:
		return nil, fmt.Errorf("invalid TIME packet length %d", len(src))
	}
	// +2 to enable negative time and 100+ hours
	dst = make([]byte, 0, length+2)
	if src[0] == 1 {
		dst = append(dst, '-')
	}
	days := binary.LittleEndian.Uint32(src[1:5])
	hours := int64(days)*24 + int64(src[5])

	if hours >= 100 {
		dst = strconv.AppendInt(dst, hours, 10)
	} else {
		dst = append(dst, digits10[hours], digits01[hours])
	}

	min, sec := src[6], src[7]
	dst = append(dst, ':',
		digits10[min], digits01[min], ':',
		digits10[sec], digits01[sec],
	)
	return appendMicrosecs(dst, src[8:], int(length)-9), nil
}

/******************************************************************************
*                       Convert from and to bytes                             *
******************************************************************************/

func uint64ToBytes(n uint64) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
		byte(n >> 32),
		byte(n >> 40),
		byte(n >> 48),
		byte(n >> 56),
	}
}

func uint64ToString(n uint64) []byte {
	var a [20]byte
	i := 20

	// U+0030 = 0
	// ...
	// U+0039 = 9

	var q uint64
	for n >= 10 {
		i--
		q = n / 10
		a[i] = uint8(n-q*10) + 0x30
		n = q
	}

	i--
	a[i] = uint8(n) + 0x30

	return a[i:]
}

// treats string value as unsigned integer representation
func stringToInt(b []byte) int {
	val := 0
	for i := range b {
		val *= 10
		val += int(b[i] - 0x30)
	}
	return val
}

func Ucs2ToUtf8(ucs2 []byte, count int) ([]byte, int, error) {
	if count <= 0 {
		return nil, 0, nil
	}
	utf8 := make([]byte, count*3)
	pos_ucs2 := 0
	pos_utf8 := 0
	ucs2size := len(ucs2)
	for i := 0; i < count; i++ {
		if ucs2[pos_ucs2] == 0 && (ucs2[pos_ucs2+1] & 0x80) == 0 {
			utf8[pos_utf8] = ucs2[pos_ucs2+1]
			pos_ucs2 += 2
			pos_utf8++
		} else if (ucs2[pos_ucs2] & 0xf8) == 0 {
			utf8[pos_utf8] = 0xc0 | ((ucs2[pos_ucs2] & 0x07) << 2) | ((ucs2[pos_ucs2+1] & 0xc0) >> 6)
			utf8[pos_utf8+1] = 0x80 | (ucs2[pos_ucs2+1] & 0x3f)
			pos_ucs2 += 2
			pos_utf8 += 2
		} else {
			utf8[pos_utf8] = 0xe0 | (ucs2[pos_ucs2] & 0xf0) >> 4
			utf8[pos_utf8+1] = 0x80 | ((ucs2[pos_ucs2] & 0x0f) << 2) | ((ucs2[pos_ucs2+1] & 0xc0) >> 6)
			utf8[pos_utf8+2] = 0x80 | (ucs2[pos_ucs2+1] & 0x3f)
			pos_ucs2 += 2
			pos_utf8 += 3
		}
		if pos_ucs2 > ucs2size {
			return utf8, pos_ucs2, io.EOF
		}
	}
	return utf8[0:pos_utf8], pos_ucs2, nil
}

func Utf8ToUcs2(utf8 []byte, utf8len int) ([]byte, int, error) {
	if utf8len <= 0 {
		utf8len = len(utf8)
		if utf8len <= 0 {
			return nil, 0, nil
		}
	}
	ucs2 := make([]byte, utf8len*2)
	count := 0
	pos_ucs2 := 0
	for pos_utf8 := 0; pos_utf8 < utf8len; pos_utf8++ {
		if (utf8[pos_utf8] & 0x80) == 0 {
			ucs2[pos_ucs2] = 0
			ucs2[pos_ucs2+1] = utf8[pos_utf8]
			pos_ucs2 += 2
		} else if (utf8[pos_utf8] & 0xe0) == 0xc0 {
			ucs2[pos_ucs2] = ((utf8[pos_utf8] & 0x1f) >> 2)
			ucs2[pos_ucs2+1] = ((utf8[pos_utf8] & 0x03) << 6) | (utf8[pos_utf8+1] & 0x3f)
			pos_utf8++
			pos_ucs2 += 2
		} else {
			ucs2[pos_ucs2] = ((utf8[pos_utf8] & 0x0f) << 4) | ((utf8[pos_utf8+1] & 0x3f) >> 2)
			ucs2[pos_ucs2+1] = ((utf8[pos_utf8+1] & 0x03) << 6) | (utf8[pos_utf8+2] & 0x3f)
			pos_utf8 += 2
			pos_ucs2 += 2
		}
		count++
		if pos_utf8 > utf8len {
//			return ucs2, pos_utf8, io.EOF
		}
	}
	return ucs2[0:pos_ucs2], count, nil
}

func writeObject(arg driver.Value, tp byte, scale int, data []byte) (int, error) {
	var t 			byte
	var v_int64		int64
	var v_float64	float64
	var v_byte		[]byte
	var v_string	string
	var v_bool		bool
	var v_time		time.Time

	pos := 1
	if arg == nil {
		data[pos-1] = 1
		return pos, nil
	} else {
		data[pos-1] = 0
	}
	if v, ok := arg.(json.RawMessage); ok {
		arg = []byte(v)
	}
	switch v := arg.(type) {
	case int64:
		t = CLOUD_TYPE_LONG
		v_int64 = int64(v)
	case uint64:
		t = CLOUD_TYPE_LONG
		v_int64 = int64(v)
	case float64:
		t = CLOUD_TYPE_FLOAT
		v_float64 = float64(v)
	case bool:
		t = CLOUD_TYPE_BOOLEAN
		v_bool = bool(v)
	case []byte:
		t = CLOUD_TYPE_BINARY
		v_byte = v
	case string:
		t = CLOUD_TYPE_CHAR
		v_string = string(v)
	case time.Time:
		t = CLOUD_TYPE_TIME
		v_time = time.Time(v)
	case json.RawMessage:
		t = 0xff
	default:
		t = 0xff
	}
	// cache types and values
	data[pos] = tp
	pos++
	switch tp {
	case CLOUD_TYPE_SINGLE_CHAR:
		data[pos] = 0
		data[pos+1] = v_byte[0]
		pos += 2
	case CLOUD_TYPE_CHAR, CLOUD_TYPE_VARCHAR:
		var byt []byte
		switch t {
		case CLOUD_TYPE_CHAR:
			byt = []byte(v_string)
		case CLOUD_TYPE_BINARY:
			byt = v_byte
		default:
			byt = []byte("?????")
		}
		tmp, n, _ := Utf8ToUcs2(byt, len(byt))
		binary.BigEndian.PutUint32(data[pos:], uint32(n))
		pos += 4
		n = copy(data[pos:], tmp)
		pos += n
	case CLOUD_TYPE_SINGLE_BYTE:
		data[pos] = v_byte[0]
		pos++
	case CLOUD_TYPE_BINARY, CLOUD_TYPE_VARBINARY:
		switch t {
		case CLOUD_TYPE_BINARY:
			n := copy(data[pos+4:], v_byte)
			binary.BigEndian.PutUint32(data[pos:], uint32(n))
			pos += (4 + n)
		}
	case CLOUD_TYPE_INTEGER, CLOUD_TYPE_TINY_INTEGER:
		switch t {
		case CLOUD_TYPE_LONG:
			binary.BigEndian.PutUint32(data[pos:], uint32(v_int64))
			pos += 4
		}
	case CLOUD_TYPE_LONG, CLOUD_TYPE_SMALL_INTEGER:
		switch t {
		case CLOUD_TYPE_LONG:
			binary.BigEndian.PutUint64(data[pos:], uint64(v_int64))
			pos += 8
		}
	case CLOUD_TYPE_FLOAT:
		switch t {
		case CLOUD_TYPE_FLOAT:
			binary.BigEndian.PutUint32(data[pos:], math.Float32bits(float32(v_float64)))
			pos += 4
		}
	case CLOUD_TYPE_DOUBLE:
		switch t {
		case CLOUD_TYPE_FLOAT:
			binary.BigEndian.PutUint64(data[pos:], math.Float64bits(v_float64))
			pos += 8
		}
	case CLOUD_TYPE_BOOLEAN:
		switch t {
		case CLOUD_TYPE_BOOLEAN:
			if v_bool == true {
				data[pos] = 1
			} else {
				data[pos] = 0
			}
		case CLOUD_TYPE_LONG:
			if v_int64 == 0 {
				data[pos] = 0
			} else {
				data[pos] = 1
			}
		}
		pos++
	case CLOUD_TYPE_TINY_DECIMAL:
		switch t {
		case CLOUD_TYPE_LONG:
			v_float64 = float64(v_int64)
			fallthrough
		case CLOUD_TYPE_FLOAT:
			d := v_float64
			for i := 0; i < scale; i++ {
				d *= 10
			}
			n := int64(d)
			binary.BigEndian.PutUint32(data[pos:], uint32(n))
			pos += 4
		}
	case CLOUD_TYPE_SMALL_DECIMAL:
		switch t {
		case CLOUD_TYPE_LONG:
			v_float64 = float64(v_int64)
			fallthrough
		case CLOUD_TYPE_FLOAT:
			d := v_float64
			for i := 0; i < scale; i++ {
				d *= 10
			}
			n := int64(d)
			binary.BigEndian.PutUint64(data[pos:], uint64(n))
			pos += 8
		}
	case CLOUD_TYPE_BIG_DECIMAL:
		var d float64
		switch t {
		case CLOUD_TYPE_LONG:
			v_float64 = float64(v_int64)
			fallthrough
		case CLOUD_TYPE_FLOAT:
			d = v_float64
			for i := 0; i < scale; i++ {
				d *= 10
			}
			n := int64(d)
			data[pos] = 0
			pos++
			binary.BigEndian.PutUint64(data[pos:], uint64(n))
			pos += 8
		case CLOUD_TYPE_CHAR:
			data[pos] = 1
			pos++
			bi, err := string2bigInt(v_string, scale)
			if err != nil {
				return pos, err
			}
			buf, err := bigInt2bytes(bi)
			if err != nil {
				return pos, err
			}
			n := copy(data[pos+4:], buf)
			binary.BigEndian.PutUint32(data[pos:pos+4], uint32(n))
			pos += (4 + n)
		}
		data[pos] = byte(scale)
		pos++
	case CLOUD_TYPE_BIG_INTEGER:
		switch t {
		case CLOUD_TYPE_LONG:
			data[pos] = 0
			pos++
			binary.BigEndian.PutUint64(data[pos:], uint64(v_int64))
			pos += 8
		case CLOUD_TYPE_CHAR:
			data[pos] = 1
			pos++
			bi, err := string2bigInt(v_string, 0)
			if err != nil {
				return pos, err
			}
			buf, err := bigInt2bytes(bi)
			if err != nil {
				return pos, err
			}
			n := copy(data[pos+4:], buf)
			binary.BigEndian.PutUint32(data[pos:pos+4], uint32(n))
			pos += (4 + n)
		}
	case CLOUD_TYPE_DATE:
		var tm time.Time
		switch t {
		case CLOUD_TYPE_TIME:
			tm = v_time
		case CLOUD_TYPE_CHAR:
			tm, _ = time.ParseInLocation("2006-01-02", v_string, time.Local)
		case CLOUD_TYPE_BINARY:
			tm, _ = time.ParseInLocation("2006-01-02", string(v_byte), time.Local)
		default:
//			tm = new(time.Time)
		}
		year, month, day := tm.Date()
		d := year * 10000 + (int(month) - 1) * 100 + day
		binary.BigEndian.PutUint32(data[pos:], uint32(d))
		pos += 4
	case CLOUD_TYPE_TIME,
		CLOUD_TYPE_TIMESTAMP:
		t, _ := time.ParseInLocation(timeFormat, v_string, time.Local)
		d := uint64(t.Unix()) * 1000 + uint64(t.Nanosecond()) / 1000000
		binary.BigEndian.PutUint64(data[pos:], uint64(d))
		pos += 8

	case CLOUD_TYPE_BLOB:

	case CLOUD_TYPE_CLOB:

	case CLOUD_TYPE_BFILE:

	}
	return 0, nil
}

func readObject(b []byte) (driver.Value, byte, int, int, error) {
	var err error
	var dest driver.Value

	scale := 0
	if b[0] != 0 {
		return nil, CLOUD_TYPE_VARCHAR, scale, 1, nil
	}
	tp := b[1]
	pos := 2
	n := 0
	//see JAVA JDBC ObjectConverter.java
	switch tp {
	case CLOUD_TYPE_SINGLE_CHAR:
		dest = b[pos:pos+1]
		pos += 2
	case CLOUD_TYPE_CHAR, CLOUD_TYPE_VARCHAR:
		count := int(binary.BigEndian.Uint32(b[pos:pos+4]))
		pos += 4
		dest, n, err = Ucs2ToUtf8(b[pos:], count)
		pos += n

	case CLOUD_TYPE_SINGLE_BYTE:
		dest = b[pos:pos]
		pos++

	case CLOUD_TYPE_BINARY, CLOUD_TYPE_VARBINARY:
		n := int(binary.BigEndian.Uint32(b[pos:pos+4]))
		pos += 4
		buf := make([]byte, n)
		copy(buf, b[pos:pos+n])
		dest = buf
		pos += n

	case CLOUD_TYPE_INTEGER, CLOUD_TYPE_TINY_INTEGER:
		dest = int32(binary.BigEndian.Uint32(b[pos:pos+4]))
		pos += 4

	case CLOUD_TYPE_LONG, CLOUD_TYPE_SMALL_INTEGER:
		dest = int64(binary.BigEndian.Uint64(b[pos:pos+8]))
		pos += 8

	case CLOUD_TYPE_FLOAT:
		dest = math.Float32frombits(binary.BigEndian.Uint32(b[pos:pos+4]))
		pos += 4

	case CLOUD_TYPE_DOUBLE:
		dest = math.Float64frombits(binary.BigEndian.Uint64(b[pos:pos+8]))
		pos += 8
	case CLOUD_TYPE_DATE:
		t := int(binary.BigEndian.Uint32(b[pos:pos+4]))
		month := time.Month((t % 10000) / 100 + 1)
		dest = time.Date(t / 10000, month, t % 100, 0, 0, 0, 0, time.Local).Format(dateFormat)
		pos += 4

	case CLOUD_TYPE_TIME:
		t := int64(binary.BigEndian.Uint64(b[pos:pos+8]))
		dest = time.Unix(t / 1000, t % 1000).Format(timeFormat)
		pos += 8
	case CLOUD_TYPE_TIMESTAMP:
		t := int64(binary.BigEndian.Uint64(b[pos:pos+8]))
		dest = time.Unix(t / 1000, t % 1000).Format(timeFormat)
		pos += 8
	case CLOUD_TYPE_BOOLEAN:
		dest = b[pos]
		pos++
	case CLOUD_TYPE_TINY_DECIMAL:
		dest = binary.BigEndian.Uint64(b[pos:pos+4])
		pos += 4
		scale = int(b[pos])
		pos++
	case CLOUD_TYPE_SMALL_DECIMAL:
		dest = int64(binary.BigEndian.Uint64(b[pos:pos+8]))
		pos += 8
		scale = int(b[pos])
		pos++
	case CLOUD_TYPE_BIG_DECIMAL:
		var bi big.Int
		if b[pos] == 0 {
			bi = * new(big.Int).SetInt64(int64(binary.BigEndian.Uint64(b[pos+1 : pos+9])))
			pos += (1 + 8)
			scale = int(b[pos])
			pos++
		} else {
			n := int(b[pos+1])
			pos += 2
			bi, err = bytes2bigInt(b[pos:pos+n])
			if err != nil {
				break
			}
			pos += n
			scale = int(b[pos])
			pos++
		}
		dest, err = bigInt2string(bi, scale)
		if err != nil {
			break
		}
		pos += (n + 1)
	case CLOUD_TYPE_BIG_INTEGER:
		if b[pos] == 0 {
			dest = int64(binary.BigEndian.Uint64(b[pos+1:pos+9]))
			pos += (1 + 8)
		} else {
			var bi big.Int
			n := int(b[pos+1])
			pos += 2
			bi, err = bytes2bigInt(b[pos:pos+n])
			if err != nil {
				break
			}
			dest, err = bigInt2string(bi, 0)
			if err != nil {
				break
			}
			pos += n
		}
/*
	case CLOUD_TYPE_YEAR_MONTH:
		string := strconv.Itoa(int32(binary.BigEndian.Uint32(b[pos:pos+4]))) + "/"
			strconv.Itoa(int32(binary.BigEndian.Uint32(b[pos+4:pos+8])))
		pos += (4 + 4)

	case CLOUD_TYPE_BLOB:
//		blobId := int64(binary.BigEndian.Uint64(b[pos:pos+8]))
		pos += 8

	case CLOUD_TYPE_LONGVARBINARY,
		CLOUD_TYPE_CLOB:
//		clobId := int64(binary.BigEndian.Uint64(b[pos:pos+8]))
		pos += 8

	case CLOUD_TYPE_LONGVARCHAR,
		CLOUD_TYPE_ARRAY:
		valueLen := int32(binary.BigEndian.Uint32(b[pos:pos+4]))
		pos += 4
		//read Comparable (valueLen)

	case CLOUD_TYPE_GROUPING:
		pos++
	case CLOUD_TYPE_X2_LONG:
		int64(binary.BigEndian.Uint64(b[pos:pos+8]))
		pos += 8
		int64(binary.BigEndian.Uint64(b[pos:pos+8]))
		pos += 8
		b[pos]
		pos++
		b[pos]
		pos++
	case CLOUD_TYPE_NULL:
		dest = nil
*/
	case CLOUD_TYPE_ZONE_AUTO_SEQUENCE:
//		int32(binary.BigEndian.Uint32(b[pos:pos+4]))
		pos += 4
//		int64(binary.BigEndian.Uint64(b[pos:pos+8]))
		pos += 8
/*
	case CLOUD_TYPE_JSON_OBJECT:
		jsonElementSize := int32(binary.BigEndian.Uint32(b[pospos+4:]))
		pos += 4
	case CLOUD_TYPE_BFILE:
*/
	default:
		err = errors.New("error readObject field type not be defined")
	}
	if err != nil {
		return nil, tp, scale, pos, err
	}
	return dest, tp, scale, pos, nil
}

// returns the string read as a bytes slice, wheter the value is NULL,
// the number of bytes read and an error, in case the string is longer than
// the input slice
func ReadLengthEncodedString(b []byte) ([]byte, bool, int, error) {
	// Get length
	length := int(binary.BigEndian.Uint32(b[0:]))
	if length <= 0 {
		return b[4:4], true, 4, nil
	}
	length += 4

	// Check data length
	if len(b) >= length {
		return b[4 : length], false, length, nil
	}
	return nil, false, 4, io.EOF
}

// returns the number of bytes skipped and an error, in case the string is
// longer than the input slice
func skipLengthEncodedString(b []byte) (int, error) {
	// Get length
	num, _, n := readLengthEncodedInteger(b)
	if num < 1 {
		return n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return n, nil
	}
	return n, io.EOF
}

// returns the number read, whether the value is NULL and the number of bytes read
func readLengthEncodedInteger(b []byte) (uint64, bool, int) {
	// See issue #349
	if len(b) == 0 {
		return 0, true, 1
	}

	switch b[0] {
	// 251: NULL
	case 0xfb:
		return 0, true, 1

	// 252: value of following 2
	case 0xfc:
		return uint64(b[1]) | uint64(b[2])<<8, false, 3

	// 253: value of following 3
	case 0xfd:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, false, 4

	// 254: value of following 8
	case 0xfe:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
				uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
				uint64(b[7])<<48 | uint64(b[8])<<56,
			false, 9
	}

	// 0-250: value of first byte
	return uint64(b[0]), false, 1
}

// reserveBuffer checks cap(buf) and expand buffer to len(buf) + appendSize.
// If cap(buf) is not enough, reallocate new buffer.
func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

// escapeBytesBackslash escapes []byte with backslashes (\)
// This escapes the contents of a string (provided as []byte) by adding backslashes before special
// characters, and turning others into specific escape sequences, such as
// turning newlines into \n and null bytes into \0.
// https://github.com/cloudwave/cloudwave-server/blob/cloudwave-5.7.5/mysys/charset.c#L823-L932
func escapeBytesBackslash(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeStringBackslash is similar to escapeBytesBackslash but for string.
func escapeStringBackslash(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeBytesQuotes escapes apostrophes in []byte by doubling them up.
// This escapes the contents of a string by doubling up any apostrophes that
// it contains. This is used when the NO_BACKSLASH_ESCAPES SQL_MODE is in
// effect on the server.
// https://github.com/cloudwave/cloudwave-server/blob/cloudwave-5.7.5/mysys/charset.c#L963-L1038
func escapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeStringQuotes is similar to escapeBytesQuotes but for string.
func escapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

/******************************************************************************
*                               Sync utils                                    *
******************************************************************************/

// noCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://github.com/golang/go/issues/8005#issuecomment-190753527
// for details.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock() {}

// Unlock is a no-op used by -copylocks checker from `go vet`.
// noCopy should implement sync.Locker from Go 1.11
// https://github.com/golang/go/commit/c2eba53e7f80df21d51285879d51ab81bcfbf6bc
// https://github.com/golang/go/issues/26165
func (*noCopy) Unlock() {}

// atomicBool is a wrapper around uint32 for usage as a boolean value with
// atomic access.
type atomicBool struct {
	_noCopy noCopy
	value   uint32
}

// IsSet returns whether the current boolean value is true
func (ab *atomicBool) IsSet() bool {
	return atomic.LoadUint32(&ab.value) > 0
}

// Set sets the value of the bool regardless of the previous value
func (ab *atomicBool) Set(value bool) {
	if value {
		atomic.StoreUint32(&ab.value, 1)
	} else {
		atomic.StoreUint32(&ab.value, 0)
	}
}

// TrySet sets the value of the bool and returns whether the value changed
func (ab *atomicBool) TrySet(value bool) bool {
	if value {
		return atomic.SwapUint32(&ab.value, 1) == 0
	}
	return atomic.SwapUint32(&ab.value, 0) > 0
}

// atomicError is a wrapper for atomically accessed error values
type atomicError struct {
	_noCopy noCopy
	value   atomic.Value
}

// Set sets the error value regardless of the previous value.
// The value must not be nil
func (ae *atomicError) Set(value error) {
	ae.value.Store(value)
}

// Value returns the current error value
func (ae *atomicError) Value() error {
	if v := ae.value.Load(); v != nil {
		// this will panic if the value doesn't implement the error interface
		return v.(error)
	}
	return nil
}

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			// TODO: support the use of Named Parameters #561
			return nil, errors.New("cloudwave: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}

func mapIsolationLevel(level driver.IsolationLevel) (string, error) {
	switch sql.IsolationLevel(level) {
	case sql.LevelRepeatableRead:
		return "REPEATABLE READ", nil
	case sql.LevelReadCommitted:
		return "READ COMMITTED", nil
	case sql.LevelReadUncommitted:
		return "READ UNCOMMITTED", nil
	case sql.LevelSerializable:
		return "SERIALIZABLE", nil
	default:
		return "", fmt.Errorf("cloudwave: unsupported isolation level: %v", level)
	}
}

func whichExecute(sql string) byte {
	var str string
	str = strings.ToLower(sql)
	buf := []byte(str)
	length := len(buf)
	for i := 0; i < length; i++ {
		if buf[i] >= 'a' && buf[i] <= 'z' {
			if (i + 9) <= length {
				str = string(buf[i : i + 9])
			}
			if strings.EqualFold(str, "CloudWave") {
				return CLOUDWAVE_SELFUSEDRIVE
			}
			if (i + 6) <= length {
				str = string(buf[i : i + 6])
			}
			break
		}
	}
	if strings.EqualFold(str, "select") {
		return	CLOUDWAVE_EXECUTE_QUERY
	} else {
		if strings.EqualFold(str, "insert") || strings.EqualFold(str, "update") || strings.EqualFold(str, "delete") {
			return CLOUDWAVE_EXECUTE_UPDATE
		}
	}
	return CLOUDWAVE_EXECUTE
}


