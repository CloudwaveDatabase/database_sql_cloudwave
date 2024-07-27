package cloudwave

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"time"
)

type Expand struct {
	//Dsn string
	Db *sql.DB

	StmtId int32
}

func (e Expand) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}
	return c.Connect(context.Background())
}

func readString(data []byte) (string, int, error) {
	bytes, _, n, err := ReadLengthEncodedString(data[0:])
	if err != nil {
		return "", n, err
	}
	str := string(bytes)
	return str, n, nil
}

func (e Expand) CreateStatement() (int32, error) {
	resExec, err := e.Db.Exec("CloudWave", CONNECTION_CREATE_STATEMENT)
	if err != nil {
		return 0, err
	}
	i, _ := resExec.RowsAffected()
	buf := PullData(int(i))
	if len(buf) < 5 {
		return 0, errors.New("result is null")
	}
	num := int32(binary.BigEndian.Uint32(buf[1:]))
	return num, err
}

func (e Expand) CloseStatement(int32) error {
	resExec, err := e.Db.Exec("CloudWave", CLOSE_STATEMENT)
	if err != nil {
		return err
	}
	i, _ := resExec.RowsAffected()
	buf := PullData(int(i))
	if len(buf) <= 0 || buf[0] != iOK {
		return errors.New("error：close stmt")
	}
	return nil
}

func (e *Expand) StreamingChatBegin(dns string) error {
	var err error
	e.Db, err = sql.Open("cloudwave", dns)
	if err != nil {
		return err
	}
	// See "Important settings" section.
	e.Db.SetConnMaxLifetime(time.Minute * 3)
	e.Db.SetMaxOpenConns(10)
	e.Db.SetMaxIdleConns(10)

	e.StmtId, err = e.CreateStatement()
	return err
}

func (e *Expand) StreamingChatEnd() {
	e.CloseStatement(e.StmtId)
	if e.Db != nil {
		e.Db.Close()
	}
	return
}

func (e *Expand) StreamingChat(inputText string) (string, error) {
	resExec, err := e.Db.Exec("CloudWave", EXECUTE_STREAMING_CHAT, uint64(e.StmtId), inputText)
	if err != nil {
		return "", err
	}
	i, _ := resExec.RowsAffected()
	buf := PullData(int(i))
	if buf == nil {
		return "", nil
	}
	str := string(buf)
	return str, err
}

func (e *Expand) ReadStreamingChatToken() (string, error) {
	resExec, err := e.Db.Exec("CloudWave", READ_STREAMING_CHAT_TOKEN)
	if err != nil {
		return "", err
	}
	i, _ := resExec.RowsAffected()
	buf := PullData(int(i))
	if buf == nil {
		return "", nil
	}
	str := string(buf)
	return str, err
}
