package sio

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/funcards/engine.io"
	"github.com/funcards/socket.io-parser/v5"
	"go.uber.org/zap"
	"net/url"
	"reflect"
	"sync"
)

var _ Socket = (*socket)(nil)

var ErrEmptyEvent = errors.New("event cannot be empty")

type (
	// AllEventListener callback for all user events received on socket
	AllEventListener func(ctx context.Context, event string, args ...any)

	// ReceivedByRemoteAcknowledgement callback for remote received acknowledgement. Called when remote client calls ack callback.
	ReceivedByRemoteAcknowledgement func(ctx context.Context, args ...any)

	// ReceivedByLocalAcknowledgement callback for local received acknowledgement. Call this method to send ack to remote client.
	ReceivedByLocalAcknowledgement func(ctx context.Context, args ...any)

	Socket interface {
		eio.Emitter

		GetSID() string
		GetNamespace() Namespace
		GetConnectData() any
		GetInitialQuery() url.Values
		GetInitialHeaders() map[string]string
		Disconnect(ctx context.Context, close bool)
		Broadcast(ctx context.Context, rooms []string, event string, args ...any)
		Send(ctx context.Context, acknowledgement ReceivedByRemoteAcknowledgement, event string, args ...any)
		JoinRooms(ctx context.Context, rooms ...string)
		LeaveRooms(ctx context.Context, rooms ...string)
		LeaveAllRooms(ctx context.Context)
		RegisterAllEventListener(listener AllEventListener)
		UnregisterAllEventListener(listener AllEventListener)
		OnEvent(ctx context.Context, packet siop.Packet)
		OnAck(ctx context.Context, packet siop.Packet)
		OnPacket(ctx context.Context, packet siop.Packet)
		OnConnect(ctx context.Context)
		OnDisconnect(ctx context.Context)
		OnClose(ctx context.Context, reason string)
		OnError(ctx context.Context, msg string)
		SendPacket(ctx context.Context, packet siop.Packet)
	}

	socket struct {
		eio.Emitter

		namespace   *NamespaceImpl
		client      Client
		adapter     Adapter
		connectData any
		log         *zap.Logger
		sid         string
		connected   bool

		emu               sync.Mutex
		allEventListeners []AllEventListener

		rmu   sync.Mutex
		rooms map[string]bool

		amu              sync.RWMutex
		acknowledgements map[uint64]ReceivedByRemoteAcknowledgement
	}
)

func NewSocket(namespace *NamespaceImpl, client Client, connectData any, logger *zap.Logger) *socket {
	return &socket{
		Emitter:           eio.NewEmitter(logger),
		namespace:         namespace,
		client:            client,
		adapter:           namespace.GetAdapter(),
		connectData:       connectData,
		log:               logger,
		sid:               eio.NewSID(),
		connected:         true,
		allEventListeners: make([]AllEventListener, 0),
		rooms:             make(map[string]bool),
		acknowledgements:  make(map[uint64]ReceivedByRemoteAcknowledgement),
	}
}

func (s *socket) GetSID() string {
	return s.sid
}

func (s *socket) GetNamespace() Namespace {
	return s.namespace
}

func (s *socket) GetConnectData() any {
	return s.connectData
}

func (s *socket) GetInitialQuery() url.Values {
	return s.client.GetInitialQuery()
}

func (s *socket) GetInitialHeaders() map[string]string {
	return s.client.GetInitialHeaders()
}

func (s *socket) Disconnect(ctx context.Context, close bool) {
	if s.connected {
		if close {
			s.client.Disconnect(ctx)
			return
		}

		s.SendPacket(ctx, siop.Packet{Type: siop.Disconnect})
		s.OnClose(ctx, "server namespace disconnect")
	}
}

func (s *socket) Broadcast(ctx context.Context, rooms []string, event string, args ...any) {
	if len(event) == 0 {
		eio.TryCancel(ctx, ErrEmptyEvent)
		return
	}

	s.adapter.Broadcast(ctx, CreateDataPacket(siop.Event, event, args...), rooms, s.GetSID())
}

func (s *socket) Send(ctx context.Context, acknowledgement ReceivedByRemoteAcknowledgement, event string, args ...any) {
	if len(event) == 0 {
		eio.TryCancel(ctx, ErrEmptyEvent)
		return
	}

	packet := CreateDataPacket(siop.Event, event, args...)

	if acknowledgement != nil {
		id := s.namespace.NextID()
		packet.ID = &id

		s.amu.Lock()
		s.acknowledgements[id] = acknowledgement
		s.amu.Unlock()
	}

	s.SendPacket(ctx, packet)
}

func (s *socket) JoinRooms(ctx context.Context, rooms ...string) {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	for _, room := range rooms {
		if _, ok := s.rooms[room]; !ok {
			s.adapter.Add(ctx, room, s)
			s.rooms[room] = true
		}
	}
}

func (s *socket) LeaveRooms(ctx context.Context, rooms ...string) {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	for _, room := range rooms {
		if _, ok := s.rooms[room]; ok {
			s.adapter.Remove(ctx, room, s)
			delete(s.rooms, room)
		}
	}
}

func (s *socket) LeaveAllRooms(ctx context.Context) {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	for room, _ := range s.rooms {
		s.adapter.Remove(ctx, room, s)
	}

	s.rooms = make(map[string]bool)
}

func (s *socket) RegisterAllEventListener(listener AllEventListener) {
	s.emu.Lock()
	defer s.emu.Unlock()

	s.allEventListeners = append(s.allEventListeners, listener)
}

func (s *socket) UnregisterAllEventListener(listener AllEventListener) {
	s.emu.Lock()
	defer s.emu.Unlock()

	ptr := reflect.ValueOf(listener).Pointer()
	newListeners := make([]AllEventListener, 0)
	for _, l := range s.allEventListeners {
		if ptr != reflect.ValueOf(l).Pointer() {
			newListeners = append(newListeners, l)
		}
	}
	s.allEventListeners = newListeners
}

func (s *socket) OnEvent(ctx context.Context, packet siop.Packet) {
	args := s.unpackData(packet.Data)

	if packet.ID != nil {
		emitArgs := make([]any, len(args)+1)
		copy(emitArgs, args)
		emitArgs[len(args)] = ReceivedByLocalAcknowledgement(func(ctx context.Context, args1 ...any) {
			ack := CreateDataPacket(siop.Ack, "", args1...)
			ack.ID = packet.ID
			s.SendPacket(ctx, ack)
		})
		args = emitArgs
	}

	event := args[0].(string)
	eventArgs := make([]any, len(args)-1)
	copy(eventArgs, args[1:])

	s.Emit(ctx, event, eventArgs...)

	s.emu.Lock()
	defer s.emu.Unlock()

	for _, listener := range s.allEventListeners {
		listener(ctx, event, eventArgs...)
	}
}

func (s *socket) OnAck(ctx context.Context, packet siop.Packet) {
	s.amu.RLock()
	ack, ok := s.acknowledgements[*packet.ID]
	s.amu.RUnlock()

	if ok {
		s.amu.Lock()
		delete(s.acknowledgements, *packet.ID)
		s.amu.Unlock()

		ack(ctx, s.unpackData(packet.Data)...)
	}
}

func (s *socket) OnPacket(ctx context.Context, packet siop.Packet) {
	switch packet.Type {
	case siop.Event, siop.BinaryEvent:
		s.OnEvent(ctx, packet)
	case siop.Ack, siop.BinaryAck:
		s.OnAck(ctx, packet)
	case siop.Disconnect:
		s.OnDisconnect(ctx)
	case siop.ConnectError:
		s.OnError(ctx, packet.Data.(string))
	}
}

func (s *socket) OnConnect(ctx context.Context) {
	s.log.Debug("sio.Socket new connection", zap.String("sid", s.GetSID()), zap.Any("connect_data", s.connectData))

	s.namespace.AddConnected(s)
	s.JoinRooms(ctx, s.GetSID())
	s.SendPacket(ctx, siop.Packet{
		Type: siop.Connect,
		Data: map[string]string{
			"sid": s.GetSID(),
		},
	})
}

func (s *socket) OnDisconnect(ctx context.Context) {
	s.OnClose(ctx, "client namespace disconnect")
}

func (s *socket) OnClose(ctx context.Context, reason string) {
	if !s.connected {
		return
	}

	s.Emit(ctx, eio.TopicDisconnecting, reason)

	s.LeaveAllRooms(ctx)
	s.namespace.Remove(s)
	s.client.Remove(s)
	s.connected = false
	s.namespace.RemoveConnected(s)

	s.Emit(ctx, eio.TopicDisconnect, reason)
}

func (s *socket) OnError(ctx context.Context, msg string) {
	s.Emit(ctx, eio.TopicError, msg)
}

func (s *socket) SendPacket(ctx context.Context, packet siop.Packet) {
	packet.Nsp = s.namespace.GetName()
	s.client.SendPacket(ctx, packet)
}

func (s *socket) unpackData(data any) (newData []any) {
	// TODO: review
	switch tmp := data.(type) {
	case string:
		_ = json.Unmarshal([]byte(tmp), &newData)
	case []byte:
		_ = json.Unmarshal(tmp, &newData)
	case []any:
		newData = tmp
	}
	return
}
