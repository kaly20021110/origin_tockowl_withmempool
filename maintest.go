package main

// import (
// 	"fmt"
// 	"io"
// 	"net"
// 	"sync"
// 	"sync/atomic"
// 	"time"
// )

// const (
// 	messageSize = 50000
// 	runDuration = 10 * time.Second
// )

// var allPorts = []string{"6000", "6001", "6002", "6003"}

// func otherPorts(myPort string) []string {
// 	var peers []string
// 	for _, p := range allPorts {
// 		if p != myPort {
// 			peers = append(peers, p)
// 		}
// 	}
// 	return peers
// }

// func startReceiver(myPort string, done <-chan struct{}, wg *sync.WaitGroup, recvCounter *atomic.Int64) {
// 	defer wg.Done()

// 	listener, err := net.Listen("tcp", ":"+myPort)
// 	if err != nil {
// 		panic(fmt.Sprintf("Listen error on port %s: %v", myPort, err))
// 	}
// 	fmt.Printf("[Port %s] Receiver started.\n", myPort)

// 	go func() {
// 		<-done
// 		listener.Close()
// 	}()

// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			select {
// 			case <-done:
// 				return
// 			default:
// 				fmt.Printf("[Port %s] Accept error: %v\n", myPort, err)
// 				continue
// 			}
// 		}
// 		go handleConn(myPort, conn, done, recvCounter)
// 	}
// }

// func handleConn(myPort string, conn net.Conn, done <-chan struct{}, recvCounter *atomic.Int64) {
// 	defer conn.Close()
// 	buf := make([]byte, messageSize)

// 	for {
// 		select {
// 		case <-done:
// 			return
// 		default:
// 		}
// 		_, err := io.ReadFull(conn, buf)
// 		if err != nil {
// 			return
// 		}
// 		recvCounter.Add(1)
// 	}
// }

// func startSender(myPort string, done <-chan struct{}, wg *sync.WaitGroup, sendCounter *atomic.Int64) {
// 	defer wg.Done()
// 	time.Sleep(2 * time.Second)

// 	peers := otherPorts(myPort)
// 	conns := []net.Conn{}

// 	for _, peer := range peers {
// 		conn, err := net.Dial("tcp", "localhost:"+peer)
// 		if err != nil {
// 			fmt.Printf("[Port %s] Dial to %s failed: %v\n", myPort, peer, err)
// 			continue
// 		}
// 		conns = append(conns, conn)
// 		defer conn.Close()
// 	}

// 	msg := make([]byte, messageSize)

// loop:
// 	for {
// 		select {
// 		case <-done:
// 			break loop
// 		default:
// 		}
// 		for _, conn := range conns {
// 			_, err := conn.Write(msg)
// 			if err != nil {
// 				fmt.Printf("[Port %s] Write failed: %v\n", myPort, err)
// 				continue
// 			}
// 			sendCounter.Add(1)
// 		}
// 	}
// }

// func main() {
// 	var wg sync.WaitGroup
// 	done := make(chan struct{})

// 	sendStats := make(map[string]*atomic.Int64)
// 	recvStats := make(map[string]*atomic.Int64)

// 	// 初始化每个节点的统计计数器
// 	for _, port := range allPorts {
// 		sendStats[port] = new(atomic.Int64)
// 		recvStats[port] = new(atomic.Int64)
// 	}

// 	for _, port := range allPorts {
// 		wg.Add(1)
// 		go startReceiver(port, done, &wg, recvStats[port])
// 	}

// 	for _, port := range allPorts {
// 		wg.Add(1)
// 		go startSender(port, done, &wg, sendStats[port])
// 	}

// 	time.Sleep(runDuration)
// 	close(done)
// 	wg.Wait()

// 	fmt.Println("\n=== Final Statistics ===")
// 	for _, port := range allPorts {
// 		sendCount := sendStats[port].Load()
// 		recvCount := recvStats[port].Load()
// 		fmt.Printf("Port %s: Sent %d msgs ≈ %.2f MB, Received %d msgs ≈ %.2f MB\n",
// 			port, sendCount, float64(sendCount*messageSize)/1024/1024,
// 			recvCount, float64(recvCount*messageSize)/1024/1024)
// 	}
// }
