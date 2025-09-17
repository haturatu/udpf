package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
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

	// Sizes & Config
	CHUNK_SIZE    = 1400
	HEADER_SIZE   = 5
	WINDOW_SIZE   = 128
	RETRY_TIMEOUT = 1 * time.Second
	FINAL_TIMEOUT = 5 * time.Second
)

// --- Sliding Window State ---
type inflightPacket struct {
	sendTime time.Time
	packet   []byte
}

var window = struct {
	sync.Mutex
	base        uint32
	nextSeqNum  uint32
	sentPackets map[uint32]inflightPacket
	done        chan struct{}
}{
	base:        1,
	nextSeqNum:  1,
	sentPackets: make(map[uint32]inflightPacket),
	done:        make(chan struct{}),
}

var conn *net.UDPConn

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go <file_or_dir_path>")
		os.Exit(1)
	}
	path := os.Args[1]

	serverAddr, err := net.ResolveUDPAddr("udp", "192.168.1.227:9000")
	if err != nil {
		fmt.Printf("Error resolving server address: %v\n", err)
		os.Exit(1)
	}

	conn, err = net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		fmt.Printf("Error dialing UDP: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	go ackReceiver()
	go retransmitter()

	fileInfo, err := os.Stat(path)
	if err != nil {
		fmt.Printf("Error getting file info: %v\n", err)
		os.Exit(1)
	}

	var transferErr error
	if fileInfo.IsDir() {
		transferErr = sendDirectory(path)
	} else {
		transferErr = sendFile(path, filepath.Base(path))
	}

	if transferErr == nil {
		transferErr = waitForAcks()
	}

	close(window.done)

	if transferErr != nil {
		fmt.Printf("An error occurred: %v\n", transferErr)
		os.Exit(1)
	}

	fmt.Println("Operation completed successfully.")
}

func ackReceiver() {
	for {
		select {
		case <-window.done:
			return
		default:
			ackBuffer := make([]byte, 1024)
			conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			n, err := conn.Read(ackBuffer)
			if err != nil {
				continue
			}
			if n == HEADER_SIZE && ackBuffer[4] == ACK {
				ackSeq := binary.BigEndian.Uint32(ackBuffer[0:4])
				window.Lock()
				if ackSeq >= window.base {
					for i := window.base; i <= ackSeq; i++ {
						delete(window.sentPackets, i)
					}
					window.base = ackSeq + 1
				}
				window.Unlock()
			}
		}
	}
}

func retransmitter() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-window.done:
			return
		case <-ticker.C:
			window.Lock()
			if p, ok := window.sentPackets[window.base]; ok && time.Since(p.sendTime) > RETRY_TIMEOUT {
				fmt.Printf("Timeout on base packet %d, re-sending window.\n", window.base)
				for i := window.base; i < window.nextSeqNum; i++ {
					if toResend, ok := window.sentPackets[i]; ok {
						conn.Write(toResend.packet)
						toResend.sendTime = time.Now()
						window.sentPackets[i] = toResend
					}
				}
			}
			window.Unlock()
		}
	}
}

func sendReliably(cmd byte, payload []byte) error {
	for {
		window.Lock()
		isFull := window.nextSeqNum >= window.base+WINDOW_SIZE
		window.Unlock()
		if !isFull {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	window.Lock()
	defer window.Unlock()

	seq := window.nextSeqNum
	packet := make([]byte, HEADER_SIZE+len(payload))
	binary.BigEndian.PutUint32(packet[0:4], seq)
	packet[4] = cmd
	copy(packet[HEADER_SIZE:], payload)

	_, err := conn.Write(packet)
	if err != nil {
		return err
	}

	window.sentPackets[seq] = inflightPacket{sendTime: time.Now(), packet: packet}
	window.nextSeqNum++
	return nil
}

func sendFile(filePath string, destPath string) error {
	if err := sendReliably(CMD_FILE_START, []byte(destPath)); err != nil {
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	chunkBuffer := make([]byte, CHUNK_SIZE)
	for {
		n, err := file.Read(chunkBuffer)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}
		if err := sendReliably(CMD_CHUNK, chunkBuffer[:n]); err != nil {
			return err
		}
	}

	return sendReliably(CMD_FILE_END, nil)
}

func sendDirectory(source string) error {
	if err := sendReliably(CMD_DIR_START, []byte(filepath.Base(source))); err != nil {
		return err
	}

	err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if relPath == "." {
			return nil
		}
		if info.IsDir() {
			return sendReliably(CMD_MKDIR, []byte(relPath))
		} else {
			return sendFile(path, relPath)
		}
	})

	if err != nil {
		return err
	}

	return sendReliably(CMD_DIR_END, nil)
}

func waitForAcks() error {
	fmt.Println("Waiting for final acknowledgments...")
	timeout := time.After(FINAL_TIMEOUT)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		window.Lock()
		remaining := window.nextSeqNum - window.base
		window.Unlock()

		if remaining == 0 {
			fmt.Println("All packets acknowledged.")
			return nil
		}

		select {
		case <-timeout:
			return fmt.Errorf("failed to receive final %d ACKs after timeout", remaining)
		case <-ticker.C:
			fmt.Printf("Waiting for final %d ACKs...\n", remaining)
		case <-window.done:
			return nil
		}
	}
}
