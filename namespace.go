package sio

import (
	"github.com/funcards/engine.io"
	"github.com/funcards/socket.io-parser/v5"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"sync"
)

var _ Namespace = (*namespace)(nil)

type Namespace interface {
	eio.Emitter

	NextAckID() uint64
	Name() string
	Server() Server
	Adapter() Adapter
	Add(client Client, data any) Socket
	Remove(sck Socket)
	AddConnected(sck Socket)
	RemoveConnected(sck Socket)
	ConnectedSockets() map[string]Socket
	Broadcast(rooms []string, event string, args ...any)
}

type namespace struct {
	eio.Emitter

	ackID       *atomic.Uint64
	name        string
	srv         Server
	adapter     Adapter
	log         *zap.Logger
	sockets     *sync.Map
	connSockets *sync.Map
}

func NewNamespace(name string, srv Server, logger *zap.Logger) *namespace {
	n := &namespace{
		Emitter:     eio.NewEmitter(),
		ackID:       atomic.NewUint64(0),
		name:        name,
		srv:         srv,
		log:         logger,
		sockets:     new(sync.Map),
		connSockets: new(sync.Map),
	}
	n.adapter = srv.AdapterFactory()(n)
	return n
}

func (n *namespace) NextAckID() uint64 {
	return n.ackID.Inc()
}

func (n *namespace) Name() string {
	return n.name
}

func (n *namespace) Server() Server {
	return n.srv
}

func (n *namespace) Adapter() Adapter {
	return n.adapter
}

func (n *namespace) Add(client Client, data any) Socket {
	sck := NewSocket(n, client, data, n.log)

	if client.Conn().State() == eio.Open {
		n.log.Debug("namespace: add new connection", zap.String("nsp", n.Name()), zap.Any("data", data))
		n.sockets.Store(sck.SID(), sck)
		sck.OnConnect()
		n.Fire(eio.TopicConnection, sck)
	}

	return sck
}

func (n *namespace) Remove(sck Socket) {
	n.sockets.Delete(sck.SID())
}

func (n *namespace) AddConnected(sck Socket) {
	n.connSockets.Store(sck.SID(), sck)
}

func (n *namespace) RemoveConnected(sck Socket) {
	n.connSockets.Delete(sck.SID())
}

func (n *namespace) ConnectedSockets() map[string]Socket {
	data := map[string]Socket{}
	n.connSockets.Range(func(key, value any) bool {
		data[key.(string)] = value.(Socket)
		return true
	})
	return data
}

func (n *namespace) Broadcast(rooms []string, event string, args ...any) {
	if len(event) == 0 {
		n.log.Warn("namespace: broadcast event is empty")
		return
	}

	packet := CreateDataPacket(siop.Event, event, args...)
	n.adapter.Broadcast(packet, rooms)
}
