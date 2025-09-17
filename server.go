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
	BUFFER_SIZE = 8192 // Use a larger buffer for efficiency
)

var KNOWN_COMMANDS = map[string]bool{
	"DIR_TRANSFER_START": true,
	"MKDIR":              true,
	"FILE_START":         true,
	"FILE_END":           true,
	"DIR_TRANSFER_END":   true,
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
		// Handle each new client session sequentially
		handleConnection(conn, remoteAddr, buffer[:n])
	}
}

func handleConnection(conn *net.UDPConn, remoteAddr *net.UDPAddr, initialData []byte) {
	fmt.Printf("\nNew connection from %s\n", remoteAddr.String())

	var currentDir string
	var currentFile *os.File


	defer func() {
		if currentFile != nil {
			currentFile.Close()
		}
	}()

	// Process the first command
	processData(initialData, conn, remoteAddr, &currentDir, &currentFile)

	// Main loop for receiving subsequent commands and data
	for {
		buffer := make([]byte, BUFFER_SIZE)
		n, _, err := conn.ReadFromUDP(buffer) // Expect data from the same client
		if err != nil {
			// This could be a client-side timeout, which is normal. The loop will just break.
			fmt.Printf("Finished session with %s or timed out: %v\n", remoteAddr.String(), err)
			return
		}

		finished := processData(buffer[:n], conn, remoteAddr, &currentDir, &currentFile)
		if finished {
			return // End of transfer
		}
	}
}

// processData parses and acts on incoming data, returning true if the session is finished
func processData(data []byte, conn *net.UDPConn, remoteAddr *net.UDPAddr, currentDir *string, currentFile **os.File) bool {
	cmd, payload := parseCommand(string(data))
	var err error

	if KNOWN_COMMANDS[cmd] {
		switch cmd {
		case "DIR_TRANSFER_START":
			*currentDir = sanitizePath(payload)
			fmt.Printf("Starting directory transfer: '%s'\n", *currentDir)
			if err = os.MkdirAll(*currentDir, 0755); err != nil {
				fmt.Printf("Error creating directory '%s': %v\n", *currentDir, err)
			} else {
				sendAck(conn, remoteAddr, "DIR_TRANSFER_START")
			}
		case "MKDIR":
			dirPath := filepath.Join(*currentDir, sanitizePath(payload))
			fmt.Printf("Creating directory: '%s'\n", dirPath)
			if err = os.MkdirAll(dirPath, 0755); err != nil {
				fmt.Printf("Error creating directory '%s': %v\n", dirPath, err)
			} else {
				sendAck(conn, remoteAddr, "MKDIR")
			}
		case "FILE_START":
			if *currentFile != nil {
				(*currentFile).Close()
			}
			if *currentDir == "" { // Handle standalone file case
				*currentDir = "."
			}
			filePath := filepath.Join(*currentDir, sanitizePath(payload))
			fmt.Printf("Receiving file: '%s'\n", filePath)
			*currentFile, err = os.Create(filePath)
			if err != nil {
				fmt.Printf("Error creating file '%s': %v\n", filePath, err)
			} else {
				sendAck(conn, remoteAddr, "FILE_START")
			}
		case "FILE_END":
			if *currentFile != nil {
				fmt.Printf("Finished receiving file: '%s'\n", (*currentFile).Name())
				(*currentFile).Close()
				*currentFile = nil
			}
			sendAck(conn, remoteAddr, "FILE_END")
		case "DIR_TRANSFER_END":
			fmt.Printf("Finished directory transfer for '%s'\n", *currentDir)
			sendAck(conn, remoteAddr, "DIR_TRANSFER_END")
			return true // Signal to end the session
		}
	} else {
		// Not a known command, so it must be file data (a chunk)
		if *currentFile != nil {
			if _, err := (*currentFile).Write(data); err != nil {
				fmt.Printf("Error writing to file '%s': %v\n", (*currentFile).Name(), err)
			} else {
				// Acknowledge the chunk
				sendAck(conn, remoteAddr, "CHUNK")
			}
		} else {
			fmt.Println("Received file data without an open file")
		}
	}
	return false // Session not finished
}

func parseCommand(data string) (string, string) {
	parts := strings.SplitN(data, ":", 2)
	// Check if the first part is a known command
	if len(parts) > 0 && KNOWN_COMMANDS[parts[0]] {
		if len(parts) == 2 {
			return parts[0], parts[1]
		}
		return parts[0], ""
	}
	// If not a known command, treat the whole thing as data (or an unknown command)
	return "", data
}

func sendAck(conn *net.UDPConn, remoteAddr *net.UDPAddr, forCmd string) {
	ackMsg := "ACK:" + forCmd
	_, err := conn.WriteToUDP([]byte(ackMsg), remoteAddr)
	if err != nil {
		fmt.Printf("Error sending ACK for '%s': %v\n", forCmd, err)
	}
}

// sanitizePath prevents directory traversal attacks by only using the final component of a path.
func sanitizePath(path string) string {
	// Clean the path to resolve any '..' and then get the final, safe component.
	return filepath.Base(filepath.Clean(path))
}
