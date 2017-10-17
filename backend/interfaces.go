// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import "net/http"

type Querier interface {
	Query(w http.ResponseWriter, req *http.Request) (err error)
	//Query2(req *http.Request) (p []byte,err error)
}

type BackendAPI interface {
	Querier
	IsActive() (b bool)
	IsWriteOnly() (b bool)
	Ping() (version string, err error)
	GetZone() (zone string)
	Write(p []byte) (err error)
	Close() (err error)

	//上面的接口还有其他实现
	Query2(req *http.Request) (p []byte, err error)
}
