package cloudwave

import (
	"encoding/binary"
	"errors"
	"os"
	"reflect"
)

type CloudBlob struct {
	connection  *cwConn
	statementId uint32
	cursorId    uint32
	id          int64
	owned       bool
	writePos    int64
}

//func getType() byte {
//	return CLOUD_TYPE_BLOB
//}

func getBlob(connection *cwConn, id int64, owned bool) *CloudBlob {
	blob := new(CloudBlob)
	blob.connection = connection

	blob.statementId = INT_MIN_VALUE
	blob.cursorId = INT_MIN_VALUE
	blob.id = id
	blob.owned = owned
	return blob
}

func (blob *CloudBlob) length() (int64, error) {
	var buf []byte
	pktLen := 25 + 4*2 + 8
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(blob.id))
	pos += 8
	blob.connection.setCommandPacket(BLOB_LENGTH, pos, data[0:25])
	// Send CMD packet
	err := blob.connection.writePacket(data[0:pos])
	if err == nil {
		buf, err = blob.connection.readResultOK()
		if err == nil {
			return int64(binary.BigEndian.Uint64(buf[1:])), nil
		}
		err = errors.New("get blob length error")
	}
	return 0, err
}

func (blob *CloudBlob) position(pattern []byte, start int64) (int64, error) {
	if start <= 0 {
		return 0, errors.New("position is less than 1")
	}
	var buf []byte
	patternlength := len(pattern)
	pktLen := 25 + 4*3 + 8*2 + patternlength
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(blob.id))
	pos += 8
	binary.BigEndian.PutUint32(data[pos:], uint32(patternlength))
	pos += 4
	copy(data[pos:], pattern)
	pos += patternlength
	binary.BigEndian.PutUint64(data[pos:], uint64(start-1))
	pos += 8
	blob.connection.setCommandPacket(BLOB_POSITION_BYTEARRAY_PATTERN, pos, data[0:25])
	// Send CMD packet
	err := blob.connection.writePacket(data[0:pos])
	if err == nil {
		buf, err = blob.connection.readResultOK()
		if err == nil {
			return int64(binary.BigEndian.Uint64(buf[1:])), nil
		}
		err = errors.New("blob position error")
	}
	return 0, err
}

func (blob *CloudBlob) getBinaryStream(position int64, length int64) error {
	if position < 0 {
		return errors.New("position is less than 1")
	} else if length < 0 {
		return errors.New("length is less than 0")
	}
	pktLen := 25 + 4*2 + 8*3
	data := make([]byte, pktLen)
	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(blob.id))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(position))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(length))
	pos += 8
	blob.connection.setCommandPacket(BLOB_GET_BINARY_STREAM, pos, data[0:25])
	// Send CMD packet
	err := blob.connection.writePacket(data[0:pos])
	if err == nil {
		_, err = blob.connection.readResultOK()
		if err == nil {
			return nil
		}
		err = errors.New("blob get stream error")
	}
	return err
}

func (blob *CloudBlob) readChunk(position int64, length int) ([]byte, error) {
	var err error
	var buf []byte

	pktLen := 25 + 4*2 + 8*2 + 4 + 1
	data := make([]byte, pktLen)
	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(blob.id))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(position))
	pos += 8
	binary.BigEndian.PutUint32(data[pos:], uint32(length))
	pos += 4
	data[pos] = 1
	pos += 1
	blob.connection.setCommandPacket(LOB_READ_BUFFER, pos, data[0:25])

	err = blob.connection.writePacket(data[0:pos])
	if err == nil {
		buf, err = blob.connection.readResultOK()
		if err == nil {
			return buf[1:], err
		}
		err = errors.New("blob get []byte error")
	}
	return nil, err
}

func (blob *CloudBlob) GetBytes() ([]byte, error) {
	length, err := blob.length()
	if err != nil {
		return nil, err
	}
	err = blob.getBinaryStream(0, length)
	if err != nil {
		return nil, err
	}
	var buf []byte

	buf, err = blob.readChunk(0, int(length))
	if err == nil {
		return buf, nil
	}
	return nil, err
}

func (blob *CloudBlob) GetBlob_File(filename string) error {
	var readedLength int64
	var buf []byte

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	maxLength, err := blob.length()
	if err != nil {
		return err
	}

	err = blob.getBinaryStream(0, maxLength)
	if err != nil {
		return err
	}
	readedLength = 0
	for readedLength < maxLength {
		readLength := INT_CHUNK_SIZE
		if (readedLength + INT_CHUNK_SIZE) > maxLength {
			readLength = int(maxLength - readedLength)
		}
		buf, err = blob.readChunk(readedLength, readLength)
		if err != nil {
			return err
		}
		file.Write(buf)
		readedLength += INT_CHUNK_SIZE
	}
	return nil
}

// 写入//

func (blob *CloudBlob) setBinaryStream(position int64) error {
	if position < 1 {
		return errors.New("position is less than 1")
	}
	position = position - 1
	pktLen := 25 + 4*2 + 8*2
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(blob.id))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(position))
	pos += 8
	blob.connection.setCommandPacket(BLOB_SET_BINARY_STREAM, pos, data[0:25])
	// Send CMD packet
	err := blob.connection.writePacket(data[0:pos])
	if err == nil {
		_, err = blob.connection.readResultOK()
		if err == nil {
			return nil
		}
	}
	return err
}

func (blob *CloudBlob) truncate(length int64) error {
	pktLen := 25 + 4*2 + 8*3
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(blob.id))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(length))
	pos += 8
	blob.connection.setCommandPacket(BLOB_TRUNCATE, pos, data[0:25])
	// Send CMD packet
	err := blob.connection.writePacket(data[0:pos])
	if err == nil {
		_, err = blob.connection.readResultOK()
		if err == nil {
			return nil
		}
	}
	return err
}

func (blob *CloudBlob) free() error {
	pktLen := 25 + 4*2 + 8
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(blob.id))
	pos += 8
	blob.connection.setCommandPacket(BLOB_FREE, pos, data[0:25])
	// Send CMD packet
	err := blob.connection.writePacket(data[0:pos])
	if err == nil {
		_, err = blob.connection.readResultOK()
		if err == nil {
			return nil
		}
	}
	return err
}

// //

func (blob *CloudBlob) write(cbuf []byte, length int) error {
	var buf []byte
	pktLen := 25 + 4*2 + 8*2 + 4 + length + 1
	data := make([]byte, pktLen)

	pos := 25
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.statementId))
	pos += 4
	binary.BigEndian.PutUint32(data[pos:], uint32(blob.cursorId))
	pos += 4
	binary.BigEndian.PutUint64(data[pos:], uint64(blob.id))
	pos += 8
	binary.BigEndian.PutUint64(data[pos:], uint64(blob.writePos))
	pos += 8
	binary.BigEndian.PutUint32(data[pos:], uint32(length))
	pos += 4
	pos += copy(data[pos:], cbuf[0:length])
	data[pos] = 1
	pos += 1
	blob.connection.setCommandPacket(LOB_WRITE_BUFFER, pos, data[0:25])
	// Send CMD packet
	err := blob.connection.writePacket(data[0:pos])
	if err == nil {
		buf, err = blob.connection.readResultOK()
		if err == nil {
			blob.writePos += int64(length)
			if blob.id == -1 {
				blob.id = int64(binary.BigEndian.Uint64(buf[1:]))
			}
			return nil
		}
		err = errors.New("blob write cbuf error")
	}
	return err
}

func (blob *CloudBlob) resolveBinaryIO(reader []byte) error {
	var err error
	readerlen := len(reader)
	err = blob.setBinaryStream(1)
	if err != nil {
		return err
	}
	blob.writePos = 0

	writeoff := 0
	for writeoff < readerlen {
		writelen := INT_CHUNK_SIZE
		if (readerlen - writeoff) < INT_CHUNK_SIZE {
			writelen = readerlen - writeoff
		}
		err = blob.write(reader[writeoff:], writelen)
		if err != nil {
			return err
		}
		writeoff += writelen
	}
	err = blob.free()
	if err == nil {
		blob.owned = true
	}
	return err
}

func (blob *CloudBlob) resolveBinaryIO_File(fileInfo os.FileInfo) error {
	readerlen := fileInfo.Size()
	pathname := reflect.ValueOf(fileInfo).Elem().FieldByName("path").String()
	file, err := os.Open(pathname)
	if err != nil {
		return err
	}
	defer file.Close()
	err = blob.setBinaryStream(1)
	if err != nil {
		return err
	}

	buffer := make([]byte, INT_CHUNK_SIZE)
	blob.writePos = 0

	writeoff := int64(0)
	for writeoff < readerlen {
		writelen, err := file.Read(buffer)
		err = blob.write(buffer, writelen)
		if err != nil {
			return err
		}
		writeoff += int64(writelen)
	}
	err = blob.free()
	if err == nil {
		blob.owned = true
	}
	return err
}
