package sio

import (
	"context"
	"github.com/funcards/engine.io"
	"github.com/funcards/socket.io-parser/v5"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
)

var _ Namespace = (*NamespaceImpl)(nil)

type (
	NamespaceProvider interface {
		CheckNamespace(nsp string) bool
	}

	Namespace interface {
		eio.Emitter

		GetName() string
		GetServer() Server
		GetAdapter() Adapter
		GetConnectedSockets() map[string]Socket
		Broadcast(ctx context.Context, rooms []string, event string, args ...any)
	}

	BaseNamespace struct {
		eio.Emitter

		name    string
		server  Server
		adapter Adapter
		log     *zap.Logger
	}

	NamespaceImpl struct {
		*BaseNamespace

		ackID uint64

		smu     sync.RWMutex
		sockets map[string]Socket

		cmu              sync.RWMutex
		connectedSockets map[string]Socket
	}
)

func NewBaseNamespace(name string, server Server, logger *zap.Logger) *BaseNamespace {
	return &BaseNamespace{
		Emitter: eio.NewEmitter(logger),
		name:    name,
		server:  server,
		log:     logger,
	}
}

func (n *BaseNamespace) GetName() string {
	return n.name
}

func (n *BaseNamespace) GetServer() Server {
	return n.server
}

func (n *BaseNamespace) GetAdapter() Adapter {
	return n.adapter
}

func NewNamespaceImpl(name string, server Server, logger *zap.Logger) *NamespaceImpl {
	n := &NamespaceImpl{
		BaseNamespace:    NewBaseNamespace(name, server, logger),
		ackID:            0,
		sockets:          make(map[string]Socket),
		connectedSockets: make(map[string]Socket),
	}
	n.adapter = server.GetAdapterFactory()(n)
	return n
}

func (n *NamespaceImpl) NextID() uint64 {
	atomic.AddUint64(&(n.ackID), 1)
	return atomic.LoadUint64(&(n.ackID))
}

func (n *NamespaceImpl) Add(ctx context.Context, client Client, data any) Socket {
	n.log.Debug("namespace new connection", zap.String("name", n.GetName()), zap.Any("connect_data", data))

	sck := NewSocket(n, client, data, n.log)

	if client.GetConnection().GetState() == eio.Open {
		n.smu.Lock()
		n.sockets[sck.GetSID()] = sck
		n.smu.Unlock()

		sck.OnConnect(ctx)
		n.Emit(ctx, eio.TopicConnect, sck)
		n.Emit(ctx, eio.TopicConnection, sck)
	}

	return sck
}

func (n *NamespaceImpl) Remove(sck Socket) {
	n.smu.Lock()
	defer n.smu.Unlock()

	delete(n.sockets, sck.GetSID())
}

func (n *NamespaceImpl) AddConnected(sck Socket) {
	n.cmu.Lock()
	defer n.cmu.Unlock()

	n.connectedSockets[sck.GetSID()] = sck
}

func (n *NamespaceImpl) RemoveConnected(sck Socket) {
	n.cmu.Lock()
	defer n.cmu.Unlock()

	delete(n.connectedSockets, sck.GetSID())
}

func (n *NamespaceImpl) GetConnectedSockets() map[string]Socket {
	n.cmu.RLock()
	defer n.cmu.RUnlock()

	data := make(map[string]Socket)
	for sid, sck := range n.connectedSockets {
		data[sid] = sck
	}
	return data
}

func (n *NamespaceImpl) Broadcast(ctx context.Context, rooms []string, event string, args ...any) {
	if len(event) == 0 {
		eio.TryCancel(ctx, ErrEmptyEvent)
		return
	}

	packet := CreateDataPacket(siop.Event, event, args...)
	n.adapter.Broadcast(ctx, packet, rooms)
}
