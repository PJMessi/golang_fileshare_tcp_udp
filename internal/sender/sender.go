package sender

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

type Sender struct {
	chunkSize        uint
	udpDiscoveryPort uint
}

func NewSender(chunkSize, udpDiscoveryPort uint) *Sender {
	return &Sender{
		chunkSize:        chunkSize,
		udpDiscoveryPort: udpDiscoveryPort,
	}
}

func (s *Sender) Handle(portStr string) error {
	ctx, ctxCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ctxCancel()

	discoveryPortInt, err := strconv.Atoi(portStr)
	if err != nil || discoveryPortInt < 0 {
		return fmt.Errorf("invalid port: %s", err)
	}

	// Broadcasts location.
	go func() {
		log.Println("<=== STARTED BROADCASTING LOCATION ===>")
		if err := s.broadcastLocation(ctx, s.udpDiscoveryPort, uint(discoveryPortInt)); err != nil {
			log.Printf("err broadcasting discovery msg: %s", err)
		}
		log.Println("<=== STOPPED BROADCASTING LOCATION ===>")
	}()

	// Creates a listener for client requests.
	listener, err := net.Listen("tcp", ":"+portStr)
	if err != nil {
		return fmt.Errorf("err starting listener: %s", err)
	}
	defer func() { _ = listener.Close() }()
	log.Printf("<=== LISTENING FOR CLIENTS @ PORT: %s ===>", portStr)

	// Makes listener listen in a loop.
	for {
		con, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("err accepting connection: %s", err)
		}
		defer func() { _ = con.Close() }()

		log.Printf("<=== CONECTED TO A RECEIVER: %s ===>", con.RemoteAddr())
		log.Printf("<=== STARTED FILE SENDING PROCESS ===>")

		_ = s.sendFile(con)
	}
}

func (s *Sender) sendFile(con net.Conn) error {
	// Requests the source file path.
	filepath, err := s.requestSourceFilePath()
	if err != nil {
		return fmt.Errorf("error while requesting the source filepath: %s", err)
	}

	// Loads the provided source file.
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("err opening file: %s", err)
	}
	defer func() { _ = file.Close() }()

	// Send the file name size first.
	if err := s.sendFileNameSize(con, file); err != nil {
		return fmt.Errorf("err sending file name size: %s", err)
	}

	// Send the filename.
	_, err = con.Write([]byte(filepath))
	if err != nil {
		log.Fatalf("err sending filename: %s", err)
	}

	// Send the content.
	if err := s.sendFileContent(con, file); err != nil {
		return fmt.Errorf("err sending file content: %s", err)
	}

	return nil
}

func (s *Sender) sendFileNameSize(con net.Conn, file *os.File) error {
	fileNameLen := uint32(len(file.Name()))
	if err := binary.Write(con, binary.LittleEndian, fileNameLen); err != nil {
		return fmt.Errorf("err sending file name size: %s", err)
	}
	return nil
}

// sendFileContent sends the content of the file through the provided connection.
func (s *Sender) sendFileContent(con net.Conn, file *os.File) error {
	// Holder for a file chunk.
	chunkHolder := make([]byte, s.chunkSize)

	totalBytesSent := 0
	for {
		// Read a chunk of the file and store in the chunk holder.
		bytesRead, err := file.Read(chunkHolder)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("err reading file chunk: %s", err)
		}

		// Send the chunk in the chunk holder.
		// Using con.Write(chunk[:n]) instead of con.Write(chunk) is important because the
		// file.Read(chunk) function doesn’t always fill the buffer completely. It returns the
		// actual number of bytes read, which can be less than the buffer size, especially in the
		// last chunk or if the file is smaller than the buffer size. con.Write(chunk) would send the
		// entire buffer, including any uninitialized or old data, leading to incorrect data
		// transmission.
		_, err = con.Write(chunkHolder[:bytesRead])
		if err != nil {
			return fmt.Errorf("err sending file chunk: %s", err)
		}

		totalBytesSent += bytesRead
	}

	log.Printf("<=== BYTES SEND TO RECEIVER: %d ===>", totalBytesSent)
	return nil
}

// requestSourceFilePath requests the user for the file to send.
func (s *Sender) requestSourceFilePath() (string, error) {
	fmt.Println("enter the filepath: ")
	var filepath string

	if _, err := fmt.Scanln(&filepath); err != nil {
		return "", fmt.Errorf("error while scanning filepath: %s", err)
	}

	return filepath, nil
}

// broadcastLocation broadcasts a discovery message every 2 seconds over UDP until the provided
// context expires.
func (s *Sender) broadcastLocation(ctx context.Context, udpDiscoveryPort, port uint) error {
	udpBroadcastIp := fmt.Sprintf("255.255.255.255:%d", udpDiscoveryPort)
	udpAddr, err := net.ResolveUDPAddr("udp", udpBroadcastIp)
	if err != nil {
		return fmt.Errorf("err resolving udp address: %s", err)
	}

	udpConn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return fmt.Errorf("err dialing udp: %s", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		default:
			message := fmt.Sprintf("DISCOVER_SENDER: %d", port)
			_, err := udpConn.Write([]byte(message))
			if err != nil {
				return fmt.Errorf("err sending discovery msg: %s", err)
			}
		}

		time.Sleep(1 * time.Second)
	}
}
