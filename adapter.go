package sio

import (
	"github.com/funcards/socket.io-parser/v5"
	"go.uber.org/zap"
	"sync"
)

var _ Adapter = (*adapter)(nil)

type AdapterFactory func(nsp Namespace) Adapter

type Adapter interface {
	Broadcast(packet siop.Packet, rooms []string, excluded ...string)
	Add(room string, sck Socket)
	Remove(room string, sck Socket)
	Clients(room string) []Socket
	Rooms(client Socket) []string
}

type adapter struct {
	nsp         Namespace
	log         *zap.Logger
	roomSockets *sync.Map //map[room][sid]Socket
	socketRooms *sync.Map //map[sid][room]bool
}

func NewAdapter(nsp Namespace, logger *zap.Logger) *adapter {
	return &adapter{
		nsp:         nsp,
		log:         logger,
		roomSockets: new(sync.Map),
		socketRooms: new(sync.Map),
	}
}

func (a *adapter) Broadcast(packet siop.Packet, rooms []string, excluded ...string) {
	a.log.Debug("adapter broadcast before send packet")

	sidExcluded := make(map[string]bool, len(excluded))
	for _, s := range excluded {
		sidExcluded[s] = true
	}
	connected := a.nsp.ConnectedSockets()

	if len(rooms) == 0 {
		a.socketRooms.Range(func(key, value any) bool {
			if _, ok := sidExcluded[key.(string)]; !ok {
				if s, ok1 := connected[key.(string)]; ok1 {
					a.log.Debug("adapter broadcast sending packet...")
					s.SendPacket(packet)
				}
			}
			return true
		})
	} else {
		sent := map[string]bool{}
		for _, room := range rooms {
			if sockets, ok := a.roomSockets.Load(room); ok {
				sockets.(*sync.Map).Range(func(key, value any) bool {
					s := value.(Socket)
					if _, ok1 := sidExcluded[s.SID()]; !ok1 {
						if _, ok2 := sent[s.SID()]; !ok2 {
							if _, ok3 := connected[s.SID()]; ok3 {
								sent[s.SID()] = true
								a.log.Debug("adapter broadcast sending packet...")
								s.SendPacket(packet)
							}
						}
					}
					return true
				})
			}
		}
	}
}

func (a *adapter) Add(room string, sck Socket) {
	sockets, _ := a.roomSockets.LoadOrStore(room, new(sync.Map))
	sockets.(*sync.Map).Store(sck.SID(), sck)

	rooms, _ := a.socketRooms.LoadOrStore(sck.SID(), new(sync.Map))
	rooms.(*sync.Map).Store(room, true)
}

func (a *adapter) Remove(room string, sck Socket) {
	if value, ok := a.roomSockets.Load(room); ok {
		value.(*sync.Map).Delete(sck.SID())
		remove := true
		value.(*sync.Map).Range(func(key, value any) bool {
			remove = false
			return false
		})
		if remove {
			a.roomSockets.Delete(room)
		}
	}
	if value, ok := a.socketRooms.Load(sck.SID()); ok {
		value.(*sync.Map).Delete(room)
		remove := true
		value.(*sync.Map).Range(func(key, value any) bool {
			remove = false
			return false
		})
		if remove {
			a.socketRooms.Delete(sck.SID())
		}
	}
}

func (a *adapter) Clients(room string) []Socket {
	data := make([]Socket, 0)
	if sockets, ok := a.roomSockets.Load(room); ok {
		sockets.(*sync.Map).Range(func(key, value any) bool {
			data = append(data, value.(Socket))
			return true
		})
	}
	return data
}

func (a *adapter) Rooms(client Socket) []string {
	data := make([]string, 0)
	if rooms, ok := a.socketRooms.Load(client.SID()); ok {
		rooms.(*sync.Map).Range(func(key, value any) bool {
			data = append(data, key.(string))
			return true
		})
	}
	return data
}
