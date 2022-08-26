// Go CloudWave Driver - A CloudWave-Driver for Go's database/sql package
//
// Copyright 2012 The Go-CloudWave-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package cloudwave

type cwResult struct {
	affectedRows int64
	insertId     int64
}

func (res *cwResult) LastInsertId() (int64, error) {
	return res.insertId, nil
}

func (res *cwResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}
