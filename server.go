package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
)

const (
	PORT        = ":9000"
	BUFFER_SIZE = 8192 // Increased buffer size for performance
)

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

		handleConnection(conn, remoteAddr, buffer[:n])
	}
}

func handleConnection(conn *net.UDPConn, remoteAddr *net.UDPAddr, initialData []byte) {
	fmt.Printf("\nNew connection from %s\n", remoteAddr.String())

	cmd, payload := parseCommand(string(initialData))

	var currentDir string
	var currentFile *os.File
	var err error

	defer func() {
		if currentFile != nil {
			currentFile.Close()
		}
	}()

	switch cmd {
	case "DIR_TRANSFER_START":
		currentDir = sanitizePath(payload)
		fmt.Printf("Starting directory transfer: '%s'\n", currentDir)
		if err = os.MkdirAll(currentDir, 0755); err != nil {
			fmt.Printf("Error creating directory '%s': %v\n", currentDir, err)
			return
		}
		sendAck(conn, remoteAddr, "DIR_TRANSFER_START")

	case "FILE_START":
		// Standalone file transfer
		currentDir = "."
		filePath := filepath.Join(currentDir, sanitizePath(payload))
		fmt.Printf("Receiving standalone file: '%s'\n", filePath)
		currentFile, err = os.Create(filePath)
		if err != nil {
			fmt.Printf("Error creating file '%s': %v\n", filePath, err)
			return
		}
		sendAck(conn, remoteAddr, "FILE_START")

	default:
		fmt.Printf("Invalid initial command: %s\n", cmd)
		return
	}

	// Main loop for receiving subsequent commands and data
	for {
		buffer := make([]byte, BUFFER_SIZE)
		n, _, err := conn.ReadFromUDP(buffer) // We expect data from the same client
		if err != nil {
			fmt.Printf("Error reading from UDP: %v\n", err)
			return // End this handler on error
		}

		data := buffer[:n]
		cmd, payload := parseCommand(string(data))

		switch cmd {
		case "MKDIR":
			dirPath := filepath.Join(currentDir, sanitizePath(payload))
			fmt.Printf("Creating directory: '%s'\n", dirPath)
			if err = os.MkdirAll(dirPath, 0755); err != nil {
				fmt.Printf("Error creating directory '%s': %v\n", dirPath, err)
			} else {
				sendAck(conn, remoteAddr, "MKDIR")
			}
		case "FILE_START":
			if currentFile != nil {
				currentFile.Close()
			}
			filePath := filepath.Join(currentDir, sanitizePath(payload))
			fmt.Printf("Receiving file: '%s'\n", filePath)
			currentFile, err = os.Create(filePath)
			if err != nil {
				fmt.Printf("Error creating file '%s': %v\n", filePath, err)
			} else {
				sendAck(conn, remoteAddr, "FILE_START")
			}
		case "FILE_END":
			if currentFile != nil {
				fmt.Printf("Finished receiving file: '%s'\n", currentFile.Name())
				currentFile.Close()
				currentFile = nil
			}
			sendAck(conn, remoteAddr, "FILE_END")
		case "DIR_TRANSFER_END":
			fmt.Printf("Finished directory transfer for '%s'\n", currentDir)
			sendAck(conn, remoteAddr, "DIR_TRANSFER_END")
			return // End of transfer
		default:
			// No command, so it must be file data
			if currentFile != nil {
				if _, err := currentFile.Write(data); err != nil {
					fmt.Printf("Error writing to file '%s': %v\n", currentFile.Name(), err)
				}
			} else {
				fmt.Println("Received file data without an open file")
			}
		}
	}
}

func parseCommand(data string) (string, string) {
	parts := strings.SplitN(data, ":", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return data, ""
}

func sendAck(conn *net.UDPConn, remoteAddr *net.UDPAddr, forCmd string) {
	ackMsg := "ACK:" + forCmd
	_, err := conn.WriteToUDP([]byte(ackMsg), remoteAddr)
	if err != nil {
		fmt.Printf("Error sending ACK for '%s': %v\n", forCmd, err)
	}
}

// sanitizePath prevents directory traversal attacks
func sanitizePath(path string) string {
	return filepath.Clean(filepath.Base(path))
}