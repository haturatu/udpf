package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
)

const (
	PORT         = ":9000"
	BUFFER_SIZE  = 4096
	ACK_FILENAME = "ACK_FILENAME"
)

func main() {
	// UDPアドレスを解決
	addr, err := net.ResolveUDPAddr("udp", PORT)
	if err != nil {
		fmt.Println("Error resolving UDP address:", err)
		os.Exit(1)
	}

	// UDPポートをリッスン
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Println("Error listening on UDP port:", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Listening on %s\n", addr.String())

	tmpBuffer := make([]byte, BUFFER_SIZE)

	// ファイル名を受信する
	n, remoteAddr, err := conn.ReadFromUDP(tmpBuffer)
	if err != nil {
		fmt.Println("Error receiving filename:", err)
		return
	}
	// セキュリティ対策: ディレクトリトラバーサルを防ぐ
	fileName := filepath.Base(string(tmpBuffer[:n]))
	fmt.Printf("Received request to save file: '%s' from %s\n", fileName, remoteAddr.String())

	// ファイル名受信の確認応答をクライアントに送信
	_, err = conn.WriteToUDP([]byte(ACK_FILENAME), remoteAddr)
	if err != nil {
		fmt.Println("Error sending ACK:", err)
		return
	}

	fmt.Println("Waiting for file content...")
	// ファイル本体を受信する
	var fileBuffer bytes.Buffer
	for {
		n, _, err := conn.ReadFromUDP(tmpBuffer)
		if err != nil {
			fmt.Println("Error reading from UDP:", err)
			continue
		}

		// EOFマーカーが含まれているかチェック
		if bytes.Contains(tmpBuffer[:n], []byte("EOF")) {
			eofIndex := bytes.Index(tmpBuffer[:n], []byte("EOF"))
			fileBuffer.Write(tmpBuffer[:eofIndex])
			fmt.Println("EOF received. Finishing reception.")
			break
		}
		fileBuffer.Write(tmpBuffer[:n])
	}

	// 受信したデータをファイルに書き出す
	err = os.WriteFile(fileName, fileBuffer.Bytes(), 0644)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully received and saved file as '%s' (Size: %d bytes)\n", fileName, fileBuffer.Len())
}
