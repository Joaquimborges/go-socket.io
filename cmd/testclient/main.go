// Soak test client — not part of the library. Run from repo root:
//
//	go run ./cmd/testclient
//	go run ./cmd/testclient -url http://localhost:8083 -token 123
//
// Env vars (optional): SOCKET_URL, SOCKET_TOKEN
//
// Ajuste emitInterval e reportInterval no topo do arquivo (ex.: 5s e 2min).
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	socketio "github.com/Joaquimborges/go-socket.io"
)

const (
	emitInterval   = 30 * time.Second
	reportInterval = 1 * time.Minute
)

type pingPayload struct {
	Username string `json:"username"`
	Message  string `json:"message"`
	Action   string `json:"action"`
}

func main() {
	log.SetFlags(0)

	defaultURL := os.Getenv("SOCKET_URL")
	defaultToken := os.Getenv("SOCKET_TOKEN")

	url := flag.String("url", defaultURL, "Socket.IO server URL")
	token := flag.String("token", defaultToken, "authentication token (Bearer)")
	flag.Parse()

	startTime := time.Now()

	var (
		connected        atomic.Bool
		connectCount     atomic.Int64
		messagesSent     atomic.Int64
		messagesReceived atomic.Int64
	)

	client, err := socketio.NewClient(*url,
		socketio.WithHeaders(http.Header{
			"Authorization": {"Bearer " + *token},
		}),
	)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	client.On("ping", func(data pingPayload) {
		messagesReceived.Add(1)
		log.Printf("[RECEIVE] ping username=%s message=%s action=%s", data.Username, data.Message, data.Action)
	})

	client.OnConnect(func() {
		connectCount.Add(1)
		connected.Store(true)
		log.Println("[CONNECT]")
	})

	client.OnDisconnect(func(err error) {
		connected.Store(false)

		reason := "unknown"
		if err != nil {
			reason = err.Error()
		}

		log.Printf("[DISCONNECT] reason=%s", reason)
	})

	client.OnReconnectAttempt(func(attempt int, backoff time.Duration, err error) {
		log.Println("[RECONNECT]")
		log.Printf("attempt=%d", attempt)
		log.Printf("backoff=%s", backoff)
		log.Printf("error=%s", err)
	})

	if err := client.Connect(); err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	emitTicker := time.NewTicker(emitInterval)
	defer emitTicker.Stop()

	reportTicker := time.NewTicker(reportInterval)
	defer reportTicker.Stop()

	for {
		select {
		case <-sig:
			_ = client.Close()
			return

		case <-emitTicker.C:
			if err := client.Emit("show_message", map[string]any{
				"username": "Pedro",
				"message":  "All good",
			}); err != nil {
				log.Printf("[EMIT] show_message failed: %v", err)
				continue
			}

			messagesSent.Add(1)
			log.Println("[EMIT] show_message")

		case <-reportTicker.C:
			printReport(
				startTime,
				connected.Load(),
				connectCount.Load(),
				messagesSent.Load(),
				messagesReceived.Load(),
			)
		}
	}
}

func printReport(
	start time.Time,
	isConnected bool,
	connectCount, sent, received int64,
) {
	var mem runtime.MemStats

	runtime.ReadMemStats(&mem)

	reconnects := connectCount - 1
	if reconnects < 0 {
		reconnects = 0
	}

	log.Println("[REPORT]")
	log.Println("========================================")
	log.Printf("Uptime: %s", formatDuration(time.Since(start)))
	log.Printf("Connected: %t", isConnected)
	log.Printf("Reconnects: %d", reconnects)
	log.Printf("Messages Sent: %d", sent)
	log.Printf("Messages Received: %d", received)
	log.Printf("Memory Alloc: %.1f MB", bytesToMB(mem.Alloc))
	log.Printf("Memory Sys: %.1f MB", bytesToMB(mem.Sys))
	log.Printf("Goroutines: %d", runtime.NumGoroutine())
	log.Println("========================================")
}

func formatDuration(d time.Duration) string {
	h := int(d.Hours())
	m := int(d.Minutes()) % 60

	if h > 0 {
		return fmt.Sprintf("%dh%dm", h, m)
	}

	return fmt.Sprintf("%dm", m)
}

func bytesToMB(b uint64) float64 {
	return float64(b) / 1024 / 1024
}
