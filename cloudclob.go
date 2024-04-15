package cloudwave

import (
	"encoding/binary"
	"errors"
	"os"
	"reflect"
)

type CloudClob struct {
	connection  *cwConn
	statementId uint32
	cursorId    uint32
	id          int64
	owned       bool
	writePos    int64
}

func getClob(connection *cwConn, id int64, owned bool) *CloudClob {
	clob := new(CloudClob)
	clob.connection = connection
	clob.statementId = INT_MIN_VALUE
	clob.cursorId = INT_MIN_VALUE
	clob.id = id
	clob.owned = owned
	return clob
}

func (clob *CloudClob) length() (int64, error) {
	var buf []byte
	pktLen := 25 + 4*2 + 8
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(clob.id))
	pos += 8
	clob.connection.setCommandPacket(CLOB_LENGTH, pos, data[0:25])
	// Send CMD packet
	err := clob.connection.writePacket(data)
	if err == nil {
		buf, err = clob.connection.readResultOK()
		if err == nil {
			return int64(binary.BigEndian.Uint64(buf[1:])), nil
		}
		err = errors.New("get clob length error")
	}
	return 0, err
}

/*
func (clob *CloudClob) position(start int64) (int64, error) {
	if start <= 0 {
		return 0, errors.New("position is less than 1")
	}
	var buf []byte
	pktLen := 25 + 4*2 + 8*3
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(clob.id))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(clob.id)) //weip ??????
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(start-1))
	pos += 8
	clob.connection.setCommandPacket(CLOB_POSITION_CLOB, pos, data[0:25])
	// Send CMD packet
	err := clob.connection.writePacket(data)
	if err == nil {
		buf, err = clob.connection.readResultOK()
		if err == nil {
			l := int64(binary.BigEndian.Uint64(buf[1:]))
			return l, nil
		}
		err = errors.New("clob position error")
	}
	return 0, err
}
*/

func (clob *CloudClob) getCharacterStream(position int64, length int64) error {
	if position < 0 {
		return errors.New("position is less than 1")
	} else if length < 0 {
		return errors.New("length is less than 0")
	}
	pktLen := 25 + 4*2 + 8*3
	data := make([]byte, pktLen)
	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(clob.id))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(position))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(length*2))
	pos += 8
	clob.connection.setCommandPacket(CLOB_GET_ASCII_STREAM, pos, data[0:25])
	// Send CMD packet
	err := clob.connection.writePacket(data)
	if err == nil {
		_, err = clob.connection.readResultOK()
		if err == nil {
			return nil
		}
		err = errors.New("clob get stream error")
	}
	return err
}

func (clob *CloudClob) readChunk(position int64, length int) ([]byte, error) {
	var err error
	var buf []byte

	pktLen := 25 + 4*2 + 8*2 + 4
	data := make([]byte, pktLen)
	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(clob.id))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(position*2))
	pos += 8
	binary.BigEndian.PutUint32(data[pos:], uint32(length*2))
	pos += 4
	clob.connection.setCommandPacket(CLOB_READ, pos, data[0:25])

	err = clob.connection.writePacket(data[0:pos])
	if err == nil {
		buf, err = clob.connection.readResultOK()
		if err == nil {
			var b []byte
			b, _, err = Ucs2ToUtf8(buf[1:], length)
			return b, err
		}
		err = errors.New("clob get string error")
	}
	return nil, err
}

func (clob *CloudClob) GetString() ([]byte, error) {
	var buf []byte

	length, err := clob.length()
	if err != nil {
		return nil, err
	}
	err = clob.getCharacterStream(0, length)
	if err != nil {
		return nil, err
	}

	buf, err = clob.readChunk(0, int(length))
	if err == nil {
		return buf, nil
	}
	return nil, err
}

func (clob *CloudClob) GetClob_File(filename string) error {
	var readedLength int64
	var buf []byte

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	maxLength, err := clob.length()
	if err != nil {
		return err
	}

	err = clob.getCharacterStream(0, maxLength)
	if err != nil {
		return err
	}
	readedLength = 0
	for readedLength < maxLength {
		readLength := INT_CHUNK_SIZE
		if (readedLength + INT_CHUNK_SIZE) > maxLength {
			readLength = int(maxLength - readedLength)
		}
		buf, err = clob.readChunk(readedLength, readLength)
		if err != nil {
			return err
		}
		file.Write(buf)
		readedLength += INT_CHUNK_SIZE
	}
	return nil
}

//写入//

func (clob *CloudClob) setAsciiStream(position int64) error {
	if position < 1 {
		return errors.New("position is less than 1")
	}
	position = position - 1
	pktLen := 25 + 4*2 + 8*2
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(clob.id))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(position))
	pos += 8
	clob.connection.setCommandPacket(CLOB_SET_ASCII_STREAM, pos, data[0:25])
	// Send CMD packet
	err := clob.connection.writePacket(data)
	if err == nil {
		_, err = clob.connection.readResultOK()
		if err == nil {
			return nil
		}
	}
	return err
}

func (clob *CloudClob) setCharacterStream(position int64) error {
	if position < 1 {
		//throw new Exception("pos is less than 1");
	}
	position = position - 1
	pktLen := 25 + 4*2 + 8*2
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(clob.id))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(position))
	pos += 8
	clob.connection.setCommandPacket(CLOB_SET_CHARACTER_STREAM, pos, data[0:25])
	// Send CMD packet
	err := clob.connection.writePacket(data)
	if err == nil {
		_, err = clob.connection.readResultOK()
		if err == nil {
			return nil
		}
	}
	return err
}

/** Truncates the CLOB value that this Clob designates to have a length of len characters. */
func (clob *CloudClob) truncate(length int64) error {
	pktLen := 25 + 4*2 + 8*3
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(clob.id))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(length))
	pos += 8
	clob.connection.setCommandPacket(CLOB_TRUNCATE, pos, data[0:25])
	// Send CMD packet
	err := clob.connection.writePacket(data)
	if err == nil {
		_, err = clob.connection.readResultOK()
		if err == nil {
			return nil
		}
	}
	return err
}

func (clob *CloudClob) free() error {
	pktLen := 25 + 4*2 + 8
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(clob.id))
	pos += 8
	clob.connection.setCommandPacket(CLOB_FREE, pos, data[0:25])
	// Send CMD packet
	err := clob.connection.writePacket(data)
	if err == nil {
		_, err = clob.connection.readResultOK()
		if err == nil {
			return nil
		}
	}
	return err
}

// //

func (clob *CloudClob) write(cbuf []byte, length int, position int) error {
	var buf []byte
	pktLen := 25 + 4*2 + 8*2 + 4 + length
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(clob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(clob.id))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(clob.writePos))
	pos += 8
	binary.BigEndian.PutUint32(data[pos:], uint32(length))
	pos += 4
	pos += copy(data[pos:], cbuf[0:length])
	clob.connection.setCommandPacket(CLOB_WRITE, pos, data[0:25])
	// Send CMD packet
	err := clob.connection.writePacket(data)
	if err == nil {
		buf, err = clob.connection.readResultOK()
		if err == nil {
			clob.writePos += int64(position)
			if clob.id == -1 {
				clob.id = int64(binary.BigEndian.Uint64(buf[1:]))
			}
			return nil
		}
		err = errors.New("clob write cbuf error")
	}
	return err
}

func (clob *CloudClob) resolveCharacterIO(reader []byte) error {
	var err error
	readerlen := len(reader)
	err = clob.setCharacterStream(1)
	if err != nil {
		return err
	}
	clob.writePos = 0

	writeoff := 0
	for writeoff < readerlen {
		writelen := 4096
		if (readerlen - writeoff) < 4096 {
			writelen = readerlen - writeoff
		}
		tmp, count, pos, err := Utf8ToUcs2(reader[writeoff:], writelen)
		len := len(tmp)
		if len <= 0 {
			break
		}
		err = clob.write(tmp, len, count)
		if err != nil {
			return err
		}
		writeoff += pos
	}
	err = clob.free()
	if err == nil {
		clob.owned = true
	}
	return err
}

func (clob *CloudClob) resolveCharacterIO_File(fileInfo os.FileInfo) error {
	readerlen := fileInfo.Size()
	pathname := reflect.ValueOf(fileInfo).Elem().FieldByName("path").String()
	file, err := os.Open(pathname)
	if err != nil {
		return err
	}
	defer file.Close()
	err = clob.setCharacterStream(1)
	if err != nil {
		return err
	}

	buffer := make([]byte, INT_CHUNK_SIZE)
	clob.writePos = 0

	writeoff := int64(0)
	start := 0
	for writeoff < readerlen {
		writelen, err := file.Read(buffer[start:])
		writelen += start
		tmp, count, pos, err := Utf8ToUcs2(buffer, writelen)
		len := len(tmp)
		if len <= 0 {
			break
		}
		err = clob.write(tmp, len, count)
		if err != nil {
			return err
		}
		for i := 0; i < writelen-pos; i++ {
			buffer[i] = buffer[pos+i]
		}
		start = writelen - pos
		writeoff += int64(pos)
	}
	err = clob.free()
	if err == nil {
		clob.owned = true
	}
	return err
}
