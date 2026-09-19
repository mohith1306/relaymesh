package node

import (
	"fmt"
	"sync"
)

type State int

const (
	StateStarting   State = iota
	StateDiscovering
	StateConnected
	StateRelaying
	StateDegraded
	StateShutdown
)

func (s State) String() string {
	switch s {
	case StateStarting:
		return "STARTING"
	case StateDiscovering:
		return "DISCOVERING"
	case StateConnected:
		return "CONNECTED"
	case StateRelaying:
		return "RELAYING"
	case StateDegraded:
		return "DEGRADED"
	case StateShutdown:
		return "SHUTDOWN"
	default:
		return "UNKNOWN"
	}
}

var validTransitions = map[State][]State{
	StateStarting:   {StateDiscovering, StateShutdown},
	StateDiscovering: {StateConnected, StateStarting, StateShutdown},
	StateConnected:  {StateRelaying, StateDegraded, StateDiscovering, StateShutdown},
	StateRelaying:   {StateConnected, StateDegraded, StateShutdown},
	StateDegraded:   {StateRelaying, StateConnected, StateShutdown},
}

type StateMachine struct {
	mu           sync.RWMutex
	current      State
	previous     State
	listeners    []func(State, State)
	listenerMu   sync.RWMutex
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		current: StateStarting,
	}
}

func (sm *StateMachine) Current() State {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.current
}

func (sm *StateMachine) Previous() State {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.previous
}

func (sm *StateMachine) Transition(to State) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	allowed, exists := validTransitions[sm.current]
	if !exists {
		return fmt.Errorf("no transitions defined for state %s", sm.current)
	}

	valid := false
	for _, s := range allowed {
		if s == to {
			valid = true
			break
		}
	}

	if !valid {
		return fmt.Errorf("invalid transition from %s to %s", sm.current, to)
	}

	sm.previous = sm.current
	sm.current = to

	sm.notifyListeners(sm.previous, sm.current)
	return nil
}

func (sm *StateMachine) OnTransition(callback func(from, to State)) {
	sm.listenerMu.Lock()
	defer sm.listenerMu.Unlock()
	sm.listeners = append(sm.listeners, callback)
}

func (sm *StateMachine) notifyListeners(from, to State) {
	sm.listenerMu.RLock()
	defer sm.listenerMu.RUnlock()
	for _, listener := range sm.listeners {
		go listener(from, to)
	}
}
