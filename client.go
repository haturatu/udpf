package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	SERVER_ADDR  = "127.0.0.1:9000"
	CHUNK_SIZE   = 4096
	ACK_FILENAME = "ACK_FILENAME"
)

func main() {
	// コマンドライン引数からファイルパスを取得
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go <file_path>")
		os.Exit(1)
	}
	filePath := os.Args[1]

	// サーバーのUDPアドレスを解決
	serverAddr, err := net.ResolveUDPAddr("udp", SERVER_ADDR)
	if err != nil {
		fmt.Println("Error resolving server address:", err)
		os.Exit(1)
	}

	// UDPコネクションを確立
	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		fmt.Println("Error dialing UDP:", err)
		os.Exit(1)
	}
	defer conn.Close()

	// 1. ファイル名を送信
	fileName := filepath.Base(filePath)
	_, err = conn.Write([]byte(fileName))
	if err != nil {
		fmt.Printf("Error sending filename: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Sent filename '%s'. Waiting for ACK...\n", fileName)

	// 2. サーバーからの確認応答を待つ
	ackBuffer := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second)) // 5秒のタイムアウト
	n, _, err := conn.ReadFromUDP(ackBuffer)
	if err != nil {
		fmt.Printf("Error receiving ACK: %v\n", err)
		os.Exit(1)
	}

	if string(ackBuffer[:n]) != ACK_FILENAME {
		fmt.Println("Received invalid ACK from server")
		os.Exit(1)
	}
	fmt.Println("ACK received. Starting file transfer...")

	// ファイルを読み込んで送信
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("Error reading file '%s': %v\n", filePath, err)
		os.Exit(1)
	}

	for i := 0; i < len(fileData); i += CHUNK_SIZE {
		end := i + CHUNK_SIZE
		if end > len(fileData) {
			end = len(fileData)
		}
		chunk := fileData[i:end]

		_, err := conn.Write(chunk)
		if err != nil {
			fmt.Printf("Error sending chunk: %v\n", err)
			os.Exit(1)
		}
	}

	// 送信完了後、EOFマーカーを送信
	_, err = conn.Write([]byte("EOF"))
	if err != nil {
		fmt.Println("Error sending EOF marker:", err)
		os.Exit(1)
	}

	fmt.Println("File sent successfully.")
}
