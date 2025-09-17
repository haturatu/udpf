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
	CHUNK_SIZE      = 1400 // MTU-safe chunk size
	HEADER_SIZE     = 5
	WINDOW_SIZE     = 128 // Number of packets to have in-flight
	RETRY_TIMEOUT   = 2 * time.Second
	MAX_FINAL_TRIES = 5
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
	ackChannel  chan uint32
	done        chan struct{}
}{
	base:        1,
	nextSeqNum:  1,
	sentPackets: make(map[uint32]inflightPacket),
	ackChannel:  make(chan uint32, WINDOW_SIZE),
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

	// Start the goroutines for managing the sliding window
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

	// Wait for the last few ACKs to come in
	waitForAcks(MAX_FINAL_TRIES)
	close(window.done) // Signal background goroutines to exit

	if transferErr != nil {
		fmt.Printf("An error occurred: %v\n", transferErr)
		os.Exit(1)
	}

	fmt.Println("Operation completed successfully.")
}

// ackReceiver runs in the background, processing incoming ACKs
func ackReceiver() {
	for {
		select {
		case <-window.done:
			return
		default:
			ackBuffer := make([]byte, 1024)
			conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, err := conn.Read(ackBuffer)
			if err != nil {
				continue // Likely a timeout, which is fine
			}
			if n == HEADER_SIZE && ackBuffer[4] == ACK {
				ackSeq := binary.BigEndian.Uint32(ackBuffer[0:4])
				window.ackChannel <- ackSeq
			}
		}
	}
}

// retransmitter runs in the background, re-sending timed-out packets
func retransmitter() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-window.done:
			return
		case <-ticker.C:
			window.Lock()
			// Go-Back-N: If the base packet has timed out, re-send the whole window
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

// sendReliably creates and sends a packet, managing the sliding window.
func sendReliably(cmd byte, payload []byte) error {
	// Block until the window has space
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

	// Process any ACKs that have come in
	processAcks()
	return nil
}

// processAcks updates the window based on received ACKs.
func processAcks() {
	for {
		select {
		case ackSeq := <-window.ackChannel:
			// If this ACK allows us to slide the window
			if ackSeq >= window.base {
				// Remove all acknowledged packets
				for i := window.base; i <= ackSeq; i++ {
					delete(window.sentPackets, i)
				}
			window.base = ackSeq + 1
			}
		default:
			return
		}
	}
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

// waitForAcks gives a grace period for the last ACKs to arrive.
func waitForAcks(retries int) {
	for i := 0; i < retries; i++ {
		window.Lock()
		remaining := len(window.sentPackets)
		base := window.base
		next := window.nextSeqNum
		window.Unlock()

		if remaining == 0 && base >= next {
			fmt.Println("All packets acknowledged.")
			return
		}
		fmt.Printf("Waiting for last %d ACKs...\n", remaining)
		processAcks()
		time.Sleep(RETRY_TIMEOUT)
	}
	fmt.Println("Finished waiting for ACKs.")
}