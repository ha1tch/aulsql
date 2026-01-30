// TDS Proxy - captures and logs TDS traffic between client and server
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
)

var (
	listenAddr = flag.String("listen", ":11433", "Address to listen on")
	targetAddr = flag.String("target", "localhost:1433", "Target server address")
)

func main() {
	flag.Parse()

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()

	log.Printf("TDS Proxy listening on %s, forwarding to %s", *listenAddr, *targetAddr)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}

		go handleConnection(clientConn)
	}
}

func handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	log.Printf("New connection from %s", clientConn.RemoteAddr())

	serverConn, err := net.Dial("tcp", *targetAddr)
	if err != nil {
		log.Printf("Failed to connect to server: %v", err)
		return
	}
	defer serverConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	// Client -> Server
	go func() {
		defer wg.Done()
		copyAndLog(serverConn, clientConn, "CLIENT->SERVER")
	}()

	// Server -> Client
	go func() {
		defer wg.Done()
		copyAndLog(clientConn, serverConn, "SERVER->CLIENT")
	}()

	wg.Wait()
	log.Printf("Connection closed")
}

func copyAndLog(dst, src net.Conn, direction string) {
	buf := make([]byte, 32768)
	for {
		n, err := src.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("%s: Read error: %v", direction, err)
			}
			return
		}

		// Log the packet
		logPacket(direction, buf[:n])

		// Forward the data
		_, err = dst.Write(buf[:n])
		if err != nil {
			log.Printf("%s: Write error: %v", direction, err)
			return
		}
	}
}

func logPacket(direction string, data []byte) {
	if len(data) == 0 {
		return
	}

	// Parse TDS header if present
	if len(data) >= 8 {
		pktType := data[0]
		status := data[1]
		length := int(data[2])<<8 | int(data[3])
		spid := int(data[4])<<8 | int(data[5])
		pktID := data[6]
		window := data[7]

		typeName := getTDSTypeName(pktType)
		
		fmt.Fprintf(os.Stderr, "\n=== %s ===\n", direction)
		fmt.Fprintf(os.Stderr, "TDS Header: type=0x%02x (%s) status=0x%02x len=%d spid=%d pktID=%d window=%d\n",
			pktType, typeName, status, length, spid, pktID, window)
		
		// Dump payload (first 64 bytes or full if smaller)
		payload := data[8:]
		if len(payload) > 64 {
			fmt.Fprintf(os.Stderr, "Payload (first 64 of %d bytes):\n%s\n", len(payload), hex.Dump(payload[:64]))
		} else if len(payload) > 0 {
			fmt.Fprintf(os.Stderr, "Payload (%d bytes):\n%s\n", len(payload), hex.Dump(payload))
		}
	} else {
		fmt.Fprintf(os.Stderr, "\n=== %s (raw, %d bytes) ===\n%s\n", direction, len(data), hex.Dump(data))
	}
}

func getTDSTypeName(pktType byte) string {
	switch pktType {
	case 0x01:
		return "SQL_BATCH"
	case 0x02:
		return "RPC_REQUEST"
	case 0x03:
		return "RPC_REPLY"
	case 0x04:
		return "REPLY"
	case 0x06:
		return "ATTENTION"
	case 0x07:
		return "BULK_LOAD"
	case 0x0E:
		return "TRANS_MGR_REQ"
	case 0x10:
		return "LOGIN7"
	case 0x11:
		return "SSPI_MESSAGE"
	case 0x12:
		return "PRELOGIN"
	case 0x16:
		return "TLS_RECORD"
	default:
		return "UNKNOWN"
	}
}