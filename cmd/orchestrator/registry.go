package main

import (
	"log/slog"
	"sync"
	"time"
)

type serverState struct {
	Hostname      string
	Online        bool
	LastSeen      time.Time
	CPUPercent    float64
	MemoryPercent float64
}

type registry struct {
	mu      sync.Mutex
	servers map[string]*serverState
}

func newRegistry() *registry {
	return &registry{servers: make(map[string]*serverState)}
}

func (r *registry) markOnline(id, hostname string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	s, ok := r.servers[id]

	if !ok {
		s = &serverState{}
		r.servers[id] = s
	}

	s.Hostname = hostname
	s.LastSeen = time.Now()
	s.Online = true
	slog.Info("server online", "server_id", id, "hostname", hostname)
}

func (r *registry) recordHeartbeat(id string, cpu, mem float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	s, ok := r.servers[id]

	if !ok {
		return
	}

	s.CPUPercent = cpu
	s.MemoryPercent = mem
	s.LastSeen = time.Now()

}

func (r *registry) markOffline(id, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	s, ok := r.servers[id]

	if !ok {
		return
	}

	if s.Online {
		s.Online = false
		slog.Warn("server offline", "server_id", id, "reason", reason)
	}

}

func (r *registry) reap(timeout time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for k, v := range r.servers {

		if v.Online && time.Since(v.LastSeen) > timeout {
			v.Online = false
			slog.Warn("server offline", "server_id", k, "reason", "heartbeat timeout")
		}

	}

}

func (r *registry) runReaper(every, timeout time.Duration) {

	ticker := time.NewTicker(every)

	for range ticker.C {
		r.reap(timeout)
	}

}
