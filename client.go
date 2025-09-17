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

	// Declare err variable to hold result of send functions
	var sendErr error
	if fileInfo.IsDir() {
		sendErr = sendDirectory(path)
	} else {
		sendErr = sendFile(path, filepath.Base(path))
	}

	if sendErr != nil {
		fmt.Printf("An error occurred: %v\n", sendErr)
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
	if err := sendCommand(fmt.Sprintf("FILE_START:%s", destPath)); err != nil {
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
		if err != nil {
			if err == io.EOF {
				break // End of file
			}
			return fmt.Errorf("error reading chunk from file: %w", err)
		}
		if n == 0 {
			break
		}

		if err := sendCommandWithData("CHUNK", chunkBuffer[:n]); err != nil {
			return fmt.Errorf("error sending chunk: %w", err)
		}
	}

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

	if err := sendCommand(fmt.Sprintf("DIR_TRANSFER_START:%s", baseDir)); err != nil {
		return err
	}

	err = filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
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
			fmt.Printf("Creating directory: %s\n", relPath)
			return sendCommand(fmt.Sprintf("MKDIR:%s", relPath))
		} else {
			return sendFile(path, relPath)
		}
	})

	if err != nil {
		return fmt.Errorf("error walking directory: %w", err)
	}

	if err := sendCommand("DIR_TRANSFER_END"); err != nil {
		return err
	}

	fmt.Println("Finished directory transfer.")
	return nil
}