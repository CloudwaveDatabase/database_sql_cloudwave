// Go CloudWave Driver - A CloudWave-Driver for Go's database/sql package
//
// Copyright 2012 The Go-CloudWave-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package cloudwave

type cwTx struct {
	mc *cwConn
}

func (tx *cwTx) Commit() (err error) {
	if tx.mc == nil || tx.mc.closed.IsSet() {
		return ErrInvalidConn
	}
	if err = tx.mc.writeCommandPacket(CONNECTION_COMMIT); err != nil {
		return tx.mc.markBadConn(err)
	}
	_, err = tx.mc.readResultOK()
	tx.mc = nil
	return
}

func (tx *cwTx) Rollback() (err error) {
	if tx.mc == nil || tx.mc.closed.IsSet() {
		return ErrInvalidConn
	}
	if err = tx.mc.writeCommandPacket(CONNECTION_ROLLBACK); err != nil {
		return tx.mc.markBadConn(err)
	}
	_, err = tx.mc.readResultOK()
	tx.mc = nil
	return
}
