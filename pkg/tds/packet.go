// Package tds implements the TDS (Tabular Data Stream) protocol for SQL Server
// compatible database servers.
//
// This package provides a server-side implementation of TDS, allowing Go applications
// to accept connections from SQL Server clients such as SSMS, sqlcmd, and applications
// using go-mssqldb, pyodbc, or other TDS client libraries.
//
// The implementation is based on observing the go-mssqldb client behaviour and the
// MS-TDS protocol specification.
package tds

import (
	"encoding/binary"
	"fmt"
	"io"
)

// PacketType identifies the type of TDS packet.
type PacketType uint8

const (
	// PacketSQLBatch is sent by client for ad-hoc SQL queries.
	PacketSQLBatch PacketType = 1

	// PacketRPCRequest is sent by client to execute stored procedures.
	PacketRPCRequest PacketType = 3

	// PacketReply is sent by server in response to client requests.
	PacketReply PacketType = 4

	// PacketAttention is sent by client to cancel a running query.
	PacketAttention PacketType = 6

	// PacketBulkLoad is sent by client for bulk insert operations.
	PacketBulkLoad PacketType = 7

	// PacketFedAuthToken is sent for federated authentication.
	PacketFedAuthToken PacketType = 8

	// PacketTransMgrReq is sent for distributed transaction management.
	PacketTransMgrReq PacketType = 14

	// PacketNormal is used for TDS 4.x login (legacy).
	PacketNormal PacketType = 15

	// PacketLogin7 is sent by client for TDS 7.x login.
	PacketLogin7 PacketType = 16

	// PacketSSPIMessage is sent for SSPI/Windows authentication.
	PacketSSPIMessage PacketType = 17

	// PacketPrelogin is sent by client to negotiate connection parameters.
	PacketPrelogin PacketType = 18
)

func (p PacketType) String() string {
	switch p {
	case PacketSQLBatch:
		return "SQL_BATCH"
	case PacketRPCRequest:
		return "RPC_REQUEST"
	case PacketReply:
		return "REPLY"
	case PacketAttention:
		return "ATTENTION"
	case PacketBulkLoad:
		return "BULK_LOAD"
	case PacketFedAuthToken:
		return "FEDAUTH_TOKEN"
	case PacketTransMgrReq:
		return "TRANS_MGR_REQ"
	case PacketNormal:
		return "NORMAL"
	case PacketLogin7:
		return "LOGIN7"
	case PacketSSPIMessage:
		return "SSPI_MESSAGE"
	case PacketPrelogin:
		return "PRELOGIN"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", p)
	}
}

// PacketStatus indicates the status of a TDS packet.
type PacketStatus uint8

const (
	// StatusNormal indicates more packets follow.
	StatusNormal PacketStatus = 0x00

	// StatusEOM indicates end of message (last packet).
	StatusEOM PacketStatus = 0x01

	// StatusIgnore indicates the packet should be ignored (used during TLS).
	StatusIgnore PacketStatus = 0x02

	// StatusResetConnection requests connection reset.
	StatusResetConnection PacketStatus = 0x08

	// StatusResetConnectionSkipTran requests reset but preserves transaction.
	StatusResetConnectionSkipTran PacketStatus = 0x10
)

// HeaderSize is the size of a TDS packet header in bytes.
const HeaderSize = 8

// DefaultPacketSize is the default TDS packet size.
const DefaultPacketSize = 4096

// MaxPacketSize is the maximum allowed TDS packet size.
const MaxPacketSize = 32767

// MinPacketSize is the minimum allowed TDS packet size.
const MinPacketSize = 512

// Header represents a TDS packet header.
type Header struct {
	Type     PacketType
	Status   PacketStatus
	Length   uint16 // Total packet length including header
	SPID     uint16 // Server Process ID
	PacketID uint8  // Packet sequence number (1-255, wraps)
	Window   uint8  // Currently unused, always 0
}

// ReadHeader reads a TDS packet header from the given reader.
func ReadHeader(r io.Reader) (Header, error) {
	var buf [HeaderSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return Header{}, err
	}

	return Header{
		Type:     PacketType(buf[0]),
		Status:   PacketStatus(buf[1]),
		Length:   binary.BigEndian.Uint16(buf[2:4]),
		SPID:     binary.BigEndian.Uint16(buf[4:6]),
		PacketID: buf[6],
		Window:   buf[7],
	}, nil
}

// Write writes the header to the given writer.
func (h Header) Write(w io.Writer) error {
	var buf [HeaderSize]byte
	buf[0] = byte(h.Type)
	buf[1] = byte(h.Status)
	binary.BigEndian.PutUint16(buf[2:4], h.Length)
	binary.BigEndian.PutUint16(buf[4:6], h.SPID)
	buf[6] = h.PacketID
	buf[7] = h.Window
	_, err := w.Write(buf[:])
	return err
}

// PayloadLength returns the length of the packet payload (excluding header).
func (h Header) PayloadLength() int {
	if h.Length <= HeaderSize {
		return 0
	}
	return int(h.Length) - HeaderSize
}

// IsLastPacket returns true if this is the last packet in the message.
func (h Header) IsLastPacket() bool {
	return h.Status&StatusEOM != 0
}
