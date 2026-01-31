// Package tds provides a TDS protocol listener for aul.
//
// This package implements server-side TDS (Tabular Data Stream) protocol
// handling, allowing aul to accept connections from SQL Server clients
// such as SSMS, sqlcmd, and applications using go-mssqldb.
package tds

import (
	"github.com/ha1tch/aul/pkg/protocol"
)

func init() {
	protocol.RegisterTDSFactory(New)
}
