// Copyright 2012 The Go-CloudWave-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// Package cloudwave provides a cloudwave driver for Go's database/sql package.
//
// The driver should be used via the database/sql package:
//
//	import "database/sql"
//	import _ "github.com/go-sql-driver/cloudwave"
//
//	db, err := sql.Open("cloudwave", "user:password@/dbname")
//
// See https://github.com/go-sql-driver/cloudwave#usage for details
package cloudwave

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net"
	"sync"
)

// CloudWaveDriver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type CloudWaveDriver struct{}

// DialFunc is a function which can be used to establish the network connection.
// Custom dial functions must be registered with RegisterDial
//
// Deprecated: users should register a DialContextFunc instead
type DialFunc func(addr string) (net.Conn, error)

// DialContextFunc is a function which can be used to establish the network connection.
// Custom dial functions must be registered with RegisterDialContext
type DialContextFunc func(ctx context.Context, addr string) (net.Conn, error)

var (
	dialsLock sync.RWMutex
	dials     map[string]DialContextFunc
)

// RegisterDialContext registers a custom dial function. It can then be used by the
// network address mynet(addr), where mynet is the registered new network.
// The current context for the connection and its address is passed to the dial function.
func RegisterDialContext(net string, dial DialContextFunc) {
	dialsLock.Lock()
	defer dialsLock.Unlock()
	if dials == nil {
		dials = make(map[string]DialContextFunc)
	}
	dials[net] = dial
}

// RegisterDial registers a custom dial function. It can then be used by the
// network address mynet(addr), where mynet is the registered new network.
// addr is passed as a parameter to the dial function.
//
// Deprecated: users should call RegisterDialContext instead
func RegisterDial(network string, dial DialFunc) {
	RegisterDialContext(network, func(_ context.Context, addr string) (net.Conn, error) {
		return dial(addr)
	})
}

// Open new Connection.
// See https://github.com/go-sql-driver/mysql#dsn-data-source-name for how
// the DSN string is formatted
func (d CloudWaveDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}
	return c.Connect(context.Background())
}

func init() {
	sql.Register("cloudwave", &CloudWaveDriver{})
}

// NewConnector returns new driver.Connector.
func NewConnector(cfg *Config) (driver.Connector, error) {
	cfg = cfg.Clone()
	// normalize the contents of cfg so calls to NewConnector have the same
	// behavior as CloudWaveDriver.OpenConnector
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	return &connector{cfg: cfg}, nil
}

// OpenConnector implements driver.DriverContext.
func (d CloudWaveDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{
		cfg: cfg,
	}, nil
}
