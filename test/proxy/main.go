// Command proxy is a TCP proxy that caps the throughput of every connection
// (per direction). Used by test/e2e.sh to emulate per-flow limited links.
//
//	proxy LISTEN_ADDR TARGET_ADDR MB_PER_SECOND
package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

func pump(dst, src net.Conn, bps float64) {
	buf := make([]byte, 32<<10)
	start := time.Now()
	var sent float64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				break
			}
			sent += float64(n)
			if ahead := time.Duration(sent/bps*float64(time.Second)) - time.Since(start); ahead > 0 {
				time.Sleep(ahead)
			}
		}
		if err != nil {
			break
		}
	}
	if c, ok := dst.(*net.TCPConn); ok {
		_ = c.CloseWrite()
	}
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: proxy LISTEN TARGET MBPS")
		os.Exit(2)
	}
	mbps, err := strconv.ParseFloat(os.Args[3], 64)
	if err != nil || mbps <= 0 {
		fmt.Fprintln(os.Stderr, "invalid MBPS")
		os.Exit(2)
	}
	l, err := net.Listen("tcp", os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for {
		c, err := l.Accept()
		if err != nil {
			continue
		}
		go func() {
			defer c.Close()
			u, err := net.Dial("tcp", os.Args[2])
			if err != nil {
				return
			}
			defer u.Close()
			done := make(chan struct{}, 2)
			go func() { pump(u, c, mbps*1e6); done <- struct{}{} }()
			go func() { pump(c, u, mbps*1e6); done <- struct{}{} }()
			<-done
			<-done
		}()
	}
}
