package sio

import (
	"context"
	"github.com/funcards/engine.io"
	"github.com/funcards/socket.io-parser/v5"
	"go.uber.org/zap"
	"sync"
)

var _ Adapter = (*memoryAdapter)(nil)

type (
	AdapterFactory func(namespace Namespace) Adapter

	Adapter interface {
		eio.Emitter

		Broadcast(ctx context.Context, packet siop.Packet, rooms []string, socketsExcluded ...string)
		Add(ctx context.Context, room string, sck Socket)
		Remove(ctx context.Context, room string, sck Socket)
		ListClients(room string) []Socket
		ListClientRooms(sck Socket) []string
	}

	memoryAdapter struct {
		eio.Emitter
		namespace Namespace
		log       *zap.Logger

		roomSockets map[string][]Socket
		rmu         sync.RWMutex

		socketRooms map[string][]string
		smu         sync.RWMutex
	}
)

func NewMemoryAdapter(namespace Namespace, logger *zap.Logger) *memoryAdapter {
	return &memoryAdapter{
		Emitter:     eio.NewEmitter(logger),
		namespace:   namespace,
		log:         logger,
		roomSockets: make(map[string][]Socket),
		socketRooms: make(map[string][]string),
	}
}

func (a *memoryAdapter) Broadcast(ctx context.Context, packet siop.Packet, rooms []string, socketsExcluded ...string) {
	excluded := make(map[string]bool, len(socketsExcluded))
	for _, s := range socketsExcluded {
		excluded[s] = true
	}

	connectedSockets := a.namespace.GetConnectedSockets()

	if len(rooms) == 0 {
		a.smu.RLock()
		defer a.smu.RUnlock()

		for sid, _ := range a.socketRooms {
			if _, ok := excluded[sid]; ok {
				continue
			}

			if s, ok := connectedSockets[sid]; ok {
				s.SendPacket(ctx, packet)
			}
		}
	} else {
		sentSocketIds := make(map[string]bool)
		for _, room := range rooms {
			a.rmu.RLock()
			sockets, ok := a.roomSockets[room]
			a.rmu.RUnlock()

			if ok {
				for _, s := range sockets {
					if _, ok1 := excluded[s.GetSID()]; ok1 {
						continue
					}
					if _, ok1 := sentSocketIds[s.GetSID()]; ok1 {
						continue
					}
					if _, ok1 := connectedSockets[s.GetSID()]; ok1 {
						sentSocketIds[s.GetSID()] = true
						s.SendPacket(ctx, packet)
					}
				}
			}
		}
	}
}

func (a *memoryAdapter) Add(_ context.Context, room string, sck Socket) {
	a.rmu.RLock()
	sockets, sok := a.roomSockets[room]
	a.rmu.RUnlock()

	if sok {
		added := false
		for _, s := range sockets {
			if s.GetSID() == sck.GetSID() {
				added = true
				break
			}
		}
		if !added {
			a.rmu.Lock()
			a.roomSockets[room] = append(a.roomSockets[room], sck)
			a.rmu.Unlock()
		}
	} else {
		a.rmu.Lock()
		a.roomSockets[room] = []Socket{sck}
		a.rmu.Unlock()
	}

	a.smu.RLock()
	rooms, rok := a.socketRooms[sck.GetSID()]
	a.smu.RUnlock()

	if rok {
		added := false
		for _, r := range rooms {
			if r == room {
				added = true
				break
			}
		}
		if !added {
			a.smu.Lock()
			a.socketRooms[sck.GetSID()] = append(a.socketRooms[sck.GetSID()], room)
			a.smu.Unlock()
		}
	} else {
		a.smu.Lock()
		a.socketRooms[sck.GetSID()] = []string{room}
		a.smu.Unlock()
	}
}

func (a *memoryAdapter) Remove(_ context.Context, room string, sck Socket) {
	a.rmu.RLock()
	sockets, sok := a.roomSockets[room]
	a.rmu.RUnlock()

	if sok {
		newSockets := make([]Socket, 0)
		for _, s := range sockets {
			if s.GetSID() != sck.GetSID() {
				newSockets = append(newSockets, s)
			}
		}
		a.rmu.Lock()
		if len(newSockets) == 0 {
			delete(a.roomSockets, room)
		} else {
			a.roomSockets[room] = newSockets
		}
		a.rmu.Unlock()
	}

	a.smu.RLock()
	rooms, rok := a.socketRooms[sck.GetSID()]
	a.smu.RUnlock()

	if rok {
		newRooms := make([]string, 0)
		for _, r := range rooms {
			if r != room {
				newRooms = append(newRooms, r)
			}
		}
		a.smu.Lock()
		if len(newRooms) == 0 {
			delete(a.socketRooms, sck.GetSID())
		} else {
			a.socketRooms[sck.GetSID()] = newRooms
		}
		a.smu.Unlock()
	}
}

func (a *memoryAdapter) ListClients(room string) []Socket {
	a.rmu.RLock()
	defer a.rmu.RUnlock()

	if sockets, ok := a.roomSockets[room]; ok {
		newSockets := make([]Socket, len(sockets))
		copy(newSockets, sockets)
		return newSockets
	}

	return []Socket{}
}

func (a *memoryAdapter) ListClientRooms(sck Socket) []string {
	a.smu.RLock()
	defer a.smu.RUnlock()

	if rooms, ok := a.socketRooms[sck.GetSID()]; ok {
		newRooms := make([]string, len(rooms))
		copy(newRooms, rooms)
		return newRooms
	}

	return []string{}
}
