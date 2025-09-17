package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	PORT        = ":9000"
	BUFFER_SIZE = 8192
	TIMEOUT     = 10 * time.Second // Timeout for a session with no activity
)

// Session holds the state for a single client connection
type Session struct {
	lastSequence  int
	currentDir    string
	currentFile   *os.File
	lastPacket    time.Time
	packetChannel chan []byte
	rootDir       string // The top-level directory for this transfer
}

// sessionManager safely manages active sessions
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

	// Packet dispatcher loop
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
			// Non-blocking send to avoid holding up the dispatcher
			select {
			case session.packetChannel <- buffer[:n]:
			default:
				fmt.Printf("Session channel full for %s. Dropping packet.\n", addrStr)
			}
		} else {
			// New session
			fmt.Printf("New connection from %s\n", addrStr)
			newSession := &Session{
				packetChannel: make(chan []byte, 100), // Buffered channel
				lastPacket:    time.Now(),
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
			s.currentFile.Close()
		}
		fmt.Printf("Session closed for %s\n", addrStr)
	}()

	for {
		select {
		case packet := <-s.packetChannel:
			s.lastPacket = time.Now()
			parts := strings.SplitN(string(packet), ":", 3)
			if len(parts) < 2 {
				fmt.Printf("Invalid packet from %s: %s\n", addrStr, string(packet))
				continue
			}

			seq, cmd, payload := parts[0], parts[1], ""
			if len(parts) > 2 {
				payload = parts[2]
			}

			// Simple ACK for now, no sequence handling yet
			sendAck(conn, remoteAddr, seq, cmd)

			switch cmd {
			case "DIR_TRANSFER_START":
				s.rootDir = sanitize(payload)
				wd, _ := os.Getwd()
				s.rootDir = filepath.Join(wd, s.rootDir)
				os.MkdirAll(s.rootDir, 0755)
			case "MKDIR":
				dirPath := safeJoin(s.rootDir, payload)
				os.MkdirAll(dirPath, 0755)
			case "FILE_START":
				if s.currentFile != nil {
					s.currentFile.Close()
				}
				filePath := safeJoin(s.rootDir, payload)
				var err error
				s.currentFile, err = os.Create(filePath)
				if err != nil {
					fmt.Printf("Error creating file '%s': %v\n", filePath, err)
				}
			case "CHUNK":
				if s.currentFile != nil {
					s.currentFile.Write([]byte(payload))
				}
			case "FILE_END":
				if s.currentFile != nil {
					s.currentFile.Close()
					s.currentFile = nil
				}
			case "DIR_TRANSFER_END":
				return // End of session
			}

		case <-time.After(TIMEOUT):
			fmt.Printf("Session timeout for %s\n", addrStr)
			return
		}
	}
}

func sendAck(conn *net.UDPConn, remoteAddr *net.UDPAddr, seq, forCmd string) {
	ackMsg := fmt.Sprintf("%s:ACK:%s", seq, forCmd)
	_, err := conn.WriteToUDP([]byte(ackMsg), remoteAddr)
	if err != nil {
		fmt.Printf("Error sending ACK for seq %s: %v\n", seq, err)
	}
}

// sanitize prevents path components from being weird (e.g. `..`)
func sanitize(path string) string {
	return filepath.Base(path)
}

// safeJoin joins a root directory and a relative path, ensuring the result is within the root.
func safeJoin(root, relPath string) string {
	relPath = sanitize(relPath)
	joined := filepath.Join(root, relPath)
	if strings.HasPrefix(joined, root) {
		return joined
	}
	// If it tries to escape, just put it in the root.
	return filepath.Join(root, filepath.Base(relPath))
}