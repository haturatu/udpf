package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	SERVER_ADDR   = "192.168.1.227:9000"
	CHUNK_SIZE    = 4096
	RETRY_TIMEOUT = 2 * time.Second
	MAX_RETRIES   = 3
)

var conn *net.UDPConn
var sequence = 0

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go <file_or_dir_path>")
		os.Exit(1)
	}
	path := os.Args[1]

	serverAddr, err := net.ResolveUDPAddr("udp", SERVER_ADDR)
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

	if transferErr != nil {
		fmt.Printf("An error occurred: %v\n", transferErr)
		os.Exit(1)
	}

	fmt.Println("Operation completed successfully.")
}

func sendReliably(cmd, payload string) error {
	sequence++
	localSeq := sequence
	packet := fmt.Sprintf("%d:%s:%s", localSeq, cmd, payload)

	for i := 0; i < MAX_RETRIES; i++ {
		_, err := conn.Write([]byte(packet))
		if err != nil {
			return fmt.Errorf("error sending packet: %w", err)
		}

		ackBuffer := make([]byte, 1024)
		conn.SetReadDeadline(time.Now().Add(RETRY_TIMEOUT))
		n, err := conn.Read(ackBuffer)
		if err != nil {
			fmt.Printf("Attempt %d: No ACK for seq %d, retrying...\n", i+1, localSeq)
			continue // Timeout, retry
		}

		ackMsg := string(ackBuffer[:n])
		expectedAck := fmt.Sprintf("%d:ACK:%s", localSeq, cmd)
		if ackMsg == expectedAck {
			return nil // Success
		}
	}
	return fmt.Errorf("failed to get ACK for seq %d after %d retries", localSeq, MAX_RETRIES)
}

func sendFile(filePath string, destPath string) error {
	fmt.Printf("Sending file: %s\n", destPath)

	if err := sendReliably("FILE_START", destPath); err != nil {
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file '%s': %w", filePath, err)
	}
	defer file.Close()

	chunkBuffer := make([]byte, CHUNK_SIZE)
	for {
		n, err := file.Read(chunkBuffer)
		if err != nil && err != io.EOF {
			return fmt.Errorf("error reading chunk from file: %w", err)
		}
		if n == 0 {
			break
		}

		if err := sendReliably("CHUNK", string(chunkBuffer[:n])); err != nil {
			return fmt.Errorf("error sending chunk: %w", err)
		}
	}

	return sendReliably("FILE_END", "")
}

func sendDirectory(source string) error {
	baseDir := filepath.Base(source)
	fmt.Printf("Starting directory transfer: %s\n", baseDir)

	if err := sendReliably("DIR_TRANSFER_START", baseDir); err != nil {
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
			return sendReliably("MKDIR", relPath)
		} else {
			return sendFile(path, relPath)
		}
	})

	if err != nil {
		return fmt.Errorf("error walking directory: %w", err)
	}

	return sendReliably("DIR_TRANSFER_END", "")
}