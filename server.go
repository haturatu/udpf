package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// --- Protocol Constants ---
const (
	// Packet Types
	CMD_DIR_START byte = 0x01
	CMD_FILE_START byte = 0x02
	CMD_CHUNK      byte = 0x03
	CMD_FILE_END   byte = 0x04
	CMD_MKDIR      byte = 0x05
	CMD_DIR_END    byte = 0x06
	ACK            byte = 0xAA

	// Sizes
	HEADER_SIZE = 5 // 4 (seq) + 1 (type)
	ACK_SIZE    = 5 // 4 (seq) + 1 (type)

	// Server Config
	PORT        = ":9000"
	BUFFER_SIZE = 2048 // Must be larger than MTU-safe chunk size
	TIMEOUT     = 30 * time.Second
)

// Session holds the state for a single client connection
type Session struct {
	expectedSequence uint32
	currentFile      *bufio.Writer
	fileHandle       *os.File
	rootDir          string
	packetChannel    chan []byte
}

var sessionManager = struct {
	sync.RWMutex
	sessions map[string]*Session
}{
	sessions: make(map[string]*Session),
}

func main() {
	addr, err := net.ResolveUDPAddr("udp", PORT)
	if err != nil {
		fmt.Printf("Error resolving UDP address: %v\n", err)
		os.Exit(1)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("Error listening on UDP port: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Server listening on %s\n", addr.String())

	for {
		buffer := make([]byte, BUFFER_SIZE)
		n, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Printf("Error reading from UDP: %v\n", err)
			continue
		}

		addrStr := remoteAddr.String()
		sessionManager.RLock()
		session, found := sessionManager.sessions[addrStr]
		sessionManager.RUnlock()

		if found {
			select {
			case session.packetChannel <- buffer[:n]:
			default:
				// Drop packet if channel is full
			}
		} else {
			newSession := &Session{
				expectedSequence: 1,
				packetChannel:    make(chan []byte, 500), // High buffer for sliding window
			}
			sessionManager.Lock()
			sessionManager.sessions[addrStr] = newSession
			sessionManager.Unlock()

			go handleSession(conn, remoteAddr, newSession)
			newSession.packetChannel <- buffer[:n]
		}
	}
}

func handleSession(conn *net.UDPConn, remoteAddr *net.UDPAddr, s *Session) {
	addrStr := remoteAddr.String()
	defer func() {
		sessionManager.Lock()
		delete(sessionManager.sessions, addrStr)
		sessionManager.Unlock()
		if s.currentFile != nil {
			s.currentFile.Flush()
			s.fileHandle.Close()
		}
		fmt.Printf("Session closed for %s\n", addrStr)
	}()

	timer := time.NewTimer(TIMEOUT)
	for {
		select {
		case packet := <-s.packetChannel:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(TIMEOUT)

			if len(packet) < HEADER_SIZE {
				continue
			}

			seq := binary.BigEndian.Uint32(packet[0:4])
			cmd := packet[4]
			payload := packet[HEADER_SIZE:]

			// Go-Back-N: Only accept in-order packets.
			if seq < s.expectedSequence {
				// It's a duplicate of a past packet, re-send ACK in case it was lost
				sendAck(conn, remoteAddr, seq)
				continue
			}
			if seq > s.expectedSequence {
				// It's a future packet, discard it and wait for the client to re-send the correct one.
				continue
			}

			// Process the in-order packet
			var shouldEnd bool
			switch cmd {
			case CMD_DIR_START:
				wd, _ := os.Getwd()
				s.rootDir = filepath.Join(wd, sanitize(string(payload)))
				os.MkdirAll(s.rootDir, 0755)
			case CMD_MKDIR:
				dirPath := safeJoin(s.rootDir, string(payload))
				os.MkdirAll(dirPath, 0755)
			case CMD_FILE_START:
				if s.currentFile != nil {
					s.currentFile.Flush()
					s.fileHandle.Close()
				}
				filePath := safeJoin(s.rootDir, string(payload))
				var err error
				s.fileHandle, err = os.Create(filePath)
				if err == nil {
					s.currentFile = bufio.NewWriter(s.fileHandle)
				} else {
					fmt.Printf("Error creating file '%s': %v\n", filePath, err)
				}
			case CMD_CHUNK:
				if s.currentFile != nil {
					s.currentFile.Write(payload)
				}
			case CMD_FILE_END:
				if s.currentFile != nil {
					s.currentFile.Flush()
					s.fileHandle.Close()
					s.currentFile = nil
					s.fileHandle = nil
				}
			case CMD_DIR_END:
				shouldEnd = true
			}

			sendAck(conn, remoteAddr, seq)
			s.expectedSequence++

			if shouldEnd {
				return
			}

		case <-timer.C:
			fmt.Printf("Session timeout for %s\n", addrStr)
			return
		}
	}
}

func sendAck(conn *net.UDPConn, remoteAddr *net.UDPAddr, seq uint32) {
	ackPacket := make([]byte, ACK_SIZE)
	binary.BigEndian.PutUint32(ackPacket[0:4], seq)
	ackPacket[4] = ACK
	conn.WriteToUDP(ackPacket, remoteAddr)
}

func sanitize(path string) string {
	cleaned := filepath.Clean(path)
	// Disallow absolute paths and directory traversal
	if filepath.IsAbs(cleaned) || strings.HasPrefix(cleaned, "..") {
		return filepath.Base(path)
	}
	return cleaned
}

func safeJoin(root, relPath string) string {
	safeRelPath := sanitize(relPath)
	joined := filepath.Join(root, safeRelPath)
	// Final check to prevent escaping the root directory
	if !strings.HasPrefix(joined, root) {
		return filepath.Join(root, filepath.Base(safeRelPath))
	}
	return joined
}
