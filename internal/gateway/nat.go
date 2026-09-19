package gateway

import (
	"net"
	"sync"
	"time"
)

type NATEntry struct {
	InternalAddr *net.UDPAddr
	ExternalAddr *net.UDPAddr
	Protocol     string
	CreatedAt    time.Time
	ExpiresAt    time.Time
}

type NATTable struct {
	entries map[string]*NATEntry
	mu      sync.RWMutex
}

func NewNATTable() *NATTable {
	return &NATTable{
		entries: make(map[string]*NATEntry),
	}
}

func (nt *NATTable) AddEntry(entry *NATEntry) string {
	nt.mu.Lock()
	defer nt.mu.Unlock()

	key := entry.InternalAddr.String()
	nt.entries[key] = entry
	return key
}

func (nt *NATTable) GetEntry(key string) (*NATEntry, bool) {
	nt.mu.RLock()
	defer nt.mu.RUnlock()

	entry, ok := nt.entries[key]
	if !ok {
		return nil, false
	}

	if time.Now().After(entry.ExpiresAt) {
		delete(nt.entries, key)
		return nil, false
	}

	return entry, true
}

func (nt *NATTable) RemoveEntry(key string) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	delete(nt.entries, key)
}

func (nt *NATTable) Cleanup() int {
	nt.mu.Lock()
	defer nt.mu.Unlock()

	removed := 0
	now := time.Now()
	for key, entry := range nt.entries {
		if now.After(entry.ExpiresAt) {
			delete(nt.entries, key)
			removed++
		}
	}
	return removed
}

func (nt *NATTable) GetExternalAddr(internal *net.UDPAddr) (*net.UDPAddr, bool) {
	nt.mu.RLock()
	defer nt.mu.RUnlock()

	key := internal.String()
	entry, ok := nt.entries[key]
	if !ok {
		return nil, false
	}

	if time.Now().After(entry.ExpiresAt) {
		delete(nt.entries, key)
		return nil, false
	}

	return entry.ExternalAddr, true
}

func (nt *NATTable) GetInternalAddr(external *net.UDPAddr) (*net.UDPAddr, bool) {
	nt.mu.RLock()
	defer nt.mu.RUnlock()

	for _, entry := range nt.entries {
		if entry.ExternalAddr.String() == external.String() {
			if time.Now().Before(entry.ExpiresAt) {
				return entry.InternalAddr, true
			}
		}
	}
	return nil, false
}
