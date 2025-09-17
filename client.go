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
	CHUNK_SIZE  = 4096 // Using a smaller chunk size for more reliable transmission
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

// sendCommand sends a command and waits for a specific ACK
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
	// Determine expected ACK, handling commands with payloads
	cmdParts := strings.Split(cmd, ":")
	expectedAck := "ACK:" + cmdParts[0]
	if ackMsg != expectedAck {
		return fmt.Errorf("received invalid ACK for command '%s': got '%s', expected '%s'", cmd, ackMsg, expectedAck)
	}
	return nil
}

// sendFile handles the logic for transferring a single file with chunk acknowledgments
func sendFile(filePath string, destPath string) error {
	fmt.Printf("Sending file: %s\n", destPath)
	// 1. Send FILE_START command
	if err := sendCommand(fmt.Sprintf("FILE_START:%s", destPath)); err != nil {
		return err
	}

	// 2. Send file data in chunks
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file '%s': %w", filePath, err)
	}
	defer file.Close()

	chunkBuffer := make([]byte, CHUNK_SIZE)
	for {
		n, err := file.Read(chunkBuffer)
		if err != nil && err != os.ErrNotExist && err.Error() != "EOF" {
			return fmt.Errorf("error reading chunk from file: %w", err)
		}
		if n == 0 {
			break
		}

		// Send the chunk and wait for ACK
		if err := sendCommandWithData("CHUNK", chunkBuffer[:n]); err != nil {
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

// sendCommandWithData sends data and waits for a generic data ACK
func sendCommandWithData(cmd string, data []byte) error {
	_, err := conn.Write(data)
	if err != nil {
		return fmt.Errorf("error sending data for command '%s': %w", cmd, err)
	}

	ackBuffer := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(TIMEOUT))
	n, err := conn.Read(ackBuffer)
	if err != nil {
		return fmt.Errorf("error receiving ACK for data command '%s': %w", cmd, err)
	}

	ackMsg := string(ackBuffer[:n])
	expectedAck := "ACK:" + cmd
	if ackMsg != expectedAck {
		return fmt.Errorf("received invalid ACK for data command '%s': got '%s'", cmd, ackMsg)
	}
	return nil
}

// sendDirectory walks a directory and sends the structure and files to the server
func sendDirectory(source string) error {
	baseDir := filepath.Base(source)
	fmt.Printf("Starting directory transfer: %s\n", baseDir)

	// 1. Send DIR_TRANSFER_START command
	if err := sendCommand(fmt.Sprintf("DIR_TRANSFER_START:%s", baseDir)); err != nil {
		return err
	}

	// 2. Walk the directory and send files/subdirs
	err = filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		// Skip the root directory itself
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
