// Spike: does a loopback TCP connection between two processes in the same
// gVisor sandbox survive checkpoint/restore as a live, consistent pair?
//
// One binary, two modes. As PID 1 (no args) it serves a line-echo server on
// 127.0.0.1:9000, serves /readyz on :80, and spawns itself with the "client"
// arg as a second process. The client dials the echo server EXACTLY ONCE and
// exchanges a heartbeat every second; any connection error is terminal and
// logged as FATAL — it never re-dials, so a broken-then-reconnected socket
// can't masquerade as a surviving one in the logs.
//
// Both processes run a 250ms wall-clock ticker; a multi-second gap between
// ticks means the sandbox was checkpointed. On detection they log the wall
// and monotonic gap sizes (does gVisor's monotonic clock jump across S/R?),
// re-resolve slack.com (validates the image-baked /etc/hosts under netgo),
// and dump /run/ate identity files (feeds the golden-identity spike).
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetOutput(os.Stderr)

	if len(os.Args) > 1 && os.Args[1] == "client" {
		runClient()
		return
	}
	runSupervisor()
}

func runSupervisor() {
	log.Printf("SUPERVISOR: starting pid=%d", os.Getpid())
	logEnvironment("SUPERVISOR")
	go restoreDetector("SUPERVISOR")

	ln, err := net.Listen("tcp", "127.0.0.1:9000")
	if err != nil {
		log.Fatalf("SUPERVISOR: listen 127.0.0.1:9000: %v", err)
	}
	go echoServer(ln)

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "ok")
		})
		log.Printf("SUPERVISOR: readyz listening on :80")
		if err := http.ListenAndServe(":80", mux); err != nil {
			log.Fatalf("SUPERVISOR: readyz server: %v", err)
		}
	}()

	cmd := exec.Command("/proc/self/exe", "client")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("SUPERVISOR: spawn client: %v", err)
	}
	log.Printf("SUPERVISOR: spawned client pid=%d", cmd.Process.Pid)

	// The client never exits on its own; if it does, that's a finding.
	err = cmd.Wait()
	log.Printf("SUPERVISOR: FATAL: client exited: %v", err)
	for {
		time.Sleep(5 * time.Second)
		log.Printf("SUPERVISOR: FATAL-MARKER: client is gone")
	}
}

func echoServer(ln net.Listener) {
	accepts := 0
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalf("SERVER: accept: %v", err)
		}
		accepts++
		// More than one accept over the actor's lifetime means the client
		// re-dialed — a spike FAILURE signal even if heartbeats resume.
		log.Printf("SERVER: accepted conn #%d from %s", accepts, conn.RemoteAddr())
		go func(c net.Conn, n int) {
			sc := bufio.NewScanner(c)
			for sc.Scan() {
				if _, err := fmt.Fprintf(c, "%s\n", sc.Text()); err != nil {
					log.Printf("SERVER: conn #%d write: %v", n, err)
					return
				}
			}
			log.Printf("SERVER: conn #%d closed: %v", n, sc.Err())
		}(conn, accepts)
	}
}

func runClient() {
	log.Printf("CLIENT: starting pid=%d", os.Getpid())
	go restoreDetector("CLIENT")

	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		fatalMarker(fmt.Sprintf("initial dial: %v", err))
	}
	local := conn.LocalAddr().String()
	log.Printf("CLIENT: dialed once, local=%s remote=%s", local, conn.RemoteAddr())

	start := time.Now()
	r := bufio.NewReader(conn)
	for seq := 1; ; seq++ {
		time.Sleep(1 * time.Second)
		if _, err := fmt.Fprintf(conn, "seq=%d\n", seq); err != nil {
			fatalMarker(fmt.Sprintf("write seq=%d on local=%s: %v", seq, local, err))
		}
		line, err := r.ReadString('\n')
		if err != nil {
			fatalMarker(fmt.Sprintf("read echo of seq=%d on local=%s: %v", seq, local, err))
		}
		log.Printf("CLIENT: heartbeat ok echo=%q local=%s mono=%s wall=%s",
			line[:len(line)-1], local, time.Since(start).Truncate(time.Millisecond),
			time.Now().Format("15:04:05.000"))
	}
}

// fatalMarker never returns: the failure must stay visible in the logs and
// must not be masked by a process restart.
func fatalMarker(msg string) {
	log.Printf("CLIENT: FATAL: loopback conn broken: %s", msg)
	for {
		time.Sleep(5 * time.Second)
		log.Printf("CLIENT: FATAL-MARKER: %s", msg)
	}
}

// restoreDetector distinguishes wall-clock jumps from monotonic-clock jumps
// across a checkpoint/restore. time.Time carries a monotonic reading that
// Sub() prefers; Round(0) strips it, forcing a wall-clock comparison.
func restoreDetector(tag string) {
	prev := time.Now()
	for {
		time.Sleep(250 * time.Millisecond)
		now := time.Now()
		monoGap := now.Sub(prev)
		wallGap := now.Round(0).Sub(prev.Round(0))
		if wallGap > 2*time.Second || monoGap > 2*time.Second {
			log.Printf("%s: RESTORE-DETECTED wallGap=%s monoGap=%s",
				tag, wallGap.Truncate(time.Millisecond), monoGap.Truncate(time.Millisecond))
			logEnvironment(tag)
		}
		prev = now
	}
}

func logEnvironment(tag string) {
	if addrs, err := net.LookupHost("slack.com"); err != nil {
		log.Printf("%s: lookup slack.com: %v", tag, err)
	} else {
		log.Printf("%s: lookup slack.com -> %v", tag, addrs)
	}
	for _, f := range []string{"/run/ate/atespace", "/run/ate/actor-id"} {
		b, err := os.ReadFile(f)
		if err != nil {
			log.Printf("%s: read %s: %v", tag, f, err)
			continue
		}
		log.Printf("%s: %s = %q", tag, f, string(b))
	}
}
