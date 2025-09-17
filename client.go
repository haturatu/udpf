package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	SERVER_ADDR = "192.168.1.227:9000"
	CHUNK_SIZE  = 4096
	TIMEOUT     = 5 * time.Second
)

var conn *net.UDPConn

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

	if fileInfo.IsDir() {
		err = sendDirectory(path)
	} else {
		err = sendFile(path, filepath.Base(path))
	}

	if err != nil {
		fmt.Printf("An error occurred: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Operation completed successfully.")
}

func sendCommand(cmd string) error {
	_, err := conn.Write([]byte(cmd))
	if err != nil {
		return fmt.Errorf("error sending command '%s': %w", cmd, err)
	}

	ackBuffer := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(TIMEOUT))
	n, err := conn.Read(ackBuffer)
	if err != nil {
		return fmt.Errorf("error receiving ACK for command '%s': %w", cmd, err)
	}

	ackMsg := string(ackBuffer[:n])
	expectedAck := "ACK:" + strings.Split(cmd, ":")[0]
	if ackMsg != expectedAck {
		return fmt.Errorf("received invalid ACK for command '%s': got '%s', expected '%s'", cmd, ackMsg, expectedAck)
	}
	return nil
}

func sendFile(filePath string, destPath string) error {
	fmt.Printf("Sending file: %s\n", destPath)
	// 1. Send FILE_START command
	if err := sendCommand(fmt.Sprintf("FILE_START:%s", destPath)); err != nil {
		return err
	}

	// 2. Send file data
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("error reading file '%s': %w", filePath, err)
	}

	for i := 0; i < len(fileData); i += CHUNK_SIZE {
		end := i + CHUNK_SIZE
		if end > len(fileData) {
			end = len(fileData)
		}
		chunk := fileData[i:end]
		_, err := conn.Write(chunk)
		if err != nil {
			return fmt.Errorf("error sending chunk: %w", err)
		}
	}

	// 3. Send FILE_END command
	if err := sendCommand("FILE_END"); err != nil {
		return err
	}
	fmt.Printf("Finished sending file: %s\n", destPath)
	return nil
}

func sendDirectory(source string) error {
	baseDir := filepath.Base(source)
	fmt.Printf("Starting directory transfer: %s\n", baseDir)

	// 1. Send DIR_TRANSFER_START command
	if err := sendCommand(fmt.Sprintf("DIR_TRANSFER_START:%s", baseDir)); err != nil {
		return err
	}

	// 2. Walk the directory and send files/subdirs
	err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		// Skip the root directory itself as it's already created by DIR_TRANSFER_START
		if relPath == "." {
			return nil
		}

		if info.IsDir() {
			fmt.Printf("Creating directory: %s\n", relPath)
			return sendCommand(fmt.Sprintf("MKDIR:%s", relPath))
		} else {
			return sendFile(path, relPath)
		}
	})

	if err != nil {
		return fmt.Errorf("error walking directory: %w", err)
	}

	// 3. Send DIR_TRANSFER_END command
	if err := sendCommand("DIR_TRANSFER_END"); err != nil {
		return err
	}

	fmt.Println("Finished directory transfer.")
	return nil
}