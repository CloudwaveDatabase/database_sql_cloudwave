package cloudwave

import (
	"encoding/binary"
	"errors"
)

type cloudClob struct {
	connection  *cwConn
	statementId uint32
	cursorId    uint32
	id          int64
	owned       bool
	maxLength   int64

	writePos int64
}

func getClob(connection *cwConn, id int64, owned bool) *cloudClob {
	clob := new(cloudClob)
	clob.connection = connection
	clob.statementId = INT_MIN_VALUE
	clob.cursorId = INT_MIN_VALUE
	clob.id = id
	clob.owned = owned
	clob.maxLength = LONG_MAX_VALUE
	return clob
}

func (clob *cloudClob) getCharacterStream(position int64, length int64) error {
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

func (clob *cloudClob) getSubString() ([]byte, error) {
	length, err := clob.length()
	if err != nil {
		return nil, err
	}
	err = clob.getCharacterStream(0, length)
	if err != nil {
		return nil, err
	}
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
	binary.BigEndian.PutUint64(data[pos:], uint64(0))
	pos += 8
	binary.BigEndian.PutUint32(data[pos:], uint32(length*2))
	pos += 4
	clob.connection.setCommandPacket(CLOB_READ, pos, data[0:25])
	// Send CMD packet
	err = clob.connection.writePacket(data)
	if err == nil {
		buf, err = clob.connection.readResultOK()
		if err == nil {
			var b []byte
			b, _, err = Ucs2ToUtf8(buf[1:], int(length))
			return b, err
		}
		err = errors.New("clob get string error")
	}
	return nil, err
}

func (clob *cloudClob) length() (int64, error) {
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
func (clob *cloudClob) position(start int64) (int64, error) {
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
//写入//
func (clob *cloudClob) setAsciiStream(position int64) error {
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

func (clob *cloudClob) setCharacterStream(position int64) error {
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

func (clob *cloudClob) setString(position int64, str string, offset int32, length int32) error {
	if position < 1 {
		//throw new Exception("pos is less than 1");
	}
	if offset < 0 {
		offset = 0
	}
	if length > (int32(len(str)) - offset) {
		length = int32(len(str)) - offset
	}

	if length > int32(clob.maxLength-position+1) {
		length = int32(clob.maxLength - position + 1)
	}
	//Writer writer = setCharacterStream(position)

	//char[] content = str.toCharArray();

	//writer.write(content, 0, len);
	return nil
}

/** Truncates the CLOB value that this Clob designates to have a length of len characters. */
func (clob *cloudClob) truncate(length int64) error {
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

func (clob *cloudClob) free() error {
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

func (clob *cloudClob) write(cbuf []byte, length int) error {
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
			clob.writePos += int64(length)
			if clob.id == -1 {
				clob.id = int64(binary.BigEndian.Uint64(buf[1:]))
			}
			return nil
		}
		err = errors.New("clob write cbuf error")
	}
	return err
}

func (clob *cloudClob) resolveCharacterIO(reader []byte) error {
	var err error
	tmp, _, err := Utf8ToUcs2(reader, len(reader))
	if err != nil {
		return err
	}
	tmplen := len(tmp)
	clob.setCharacterStream(1)
	writeoff := 0
	for writeoff < tmplen {
		writelen := 4096
		if (tmplen - writeoff) < 4096 {
			writelen = tmplen - writeoff
		}
		err = clob.write(tmp[writeoff:], writelen)
		if err != nil {
			return err
		}
		writeoff += writelen
	}
	err = clob.free()
	if err == nil {
		clob.owned = true
	}
	return err
}
