package sio

import (
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
	AllEventListener func(event string, args ...any) error

	// ReceivedByRemoteAcknowledgement callback for remote received acknowledgement. Called when remote client calls ack callback.
	ReceivedByRemoteAcknowledgement func(args ...any) error

	// ReceivedByLocalAcknowledgement callback for local received acknowledgement. Call this method to send ack to remote client.
	ReceivedByLocalAcknowledgement func(args ...any) error

	Socket interface {
		eio.Emitter

		GetSID() string
		GetNamespace() Namespace
		GetConnectData() any
		GetInitialQuery() url.Values
		GetInitialHeaders() map[string]string
		Disconnect(close bool) error
		Broadcast(rooms []string, event string, args ...any) error
		Send(acknowledgement ReceivedByRemoteAcknowledgement, event string, args ...any) error
		JoinRooms(rooms ...string) error
		LeaveRooms(rooms ...string) error
		LeaveAllRooms() error
		RegisterAllEventListener(listener AllEventListener)
		UnregisterAllEventListener(listener AllEventListener)
		OnEvent(packet siop.Packet) error
		OnAck(packet siop.Packet) error
		OnPacket(packet siop.Packet) error
		OnConnect() error
		OnDisconnect() error
		OnClose(reason string) error
		OnError(msg string) error
		SendPacket(packet siop.Packet) error
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

func (s *socket) Disconnect(close bool) error {
	if s.connected {
		if close {
			return s.client.Disconnect()
		}

		if err := s.SendPacket(siop.Packet{Type: siop.Disconnect}); err != nil {
			s.log.Warn("socket send packet", zap.Error(err))
		}

		return s.OnClose("server namespace disconnect")
	}
	return nil
}

func (s *socket) Broadcast(rooms []string, event string, args ...any) error {
	if len(event) == 0 {
		return ErrEmptyEvent
	}

	return s.adapter.Broadcast(CreateDataPacket(siop.Event, event, args...), rooms, s.GetSID())
}

func (s *socket) Send(acknowledgement ReceivedByRemoteAcknowledgement, event string, args ...any) error {
	if len(event) == 0 {
		return ErrEmptyEvent
	}

	packet := CreateDataPacket(siop.Event, event, args...)

	if acknowledgement != nil {
		id := s.namespace.NextID()
		packet.ID = &id

		s.amu.Lock()
		s.acknowledgements[id] = acknowledgement
		s.amu.Unlock()
	}

	return s.SendPacket(packet)
}

func (s *socket) JoinRooms(rooms ...string) error {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	for _, room := range rooms {
		if _, ok := s.rooms[room]; !ok {
			if err := s.adapter.Add(room, s); err != nil {
				return err
			}
			s.rooms[room] = true
		}
	}
	return nil
}

func (s *socket) LeaveRooms(rooms ...string) error {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	for _, room := range rooms {
		if _, ok := s.rooms[room]; ok {
			if err := s.adapter.Remove(room, s); err != nil {
				return err
			}
			delete(s.rooms, room)
		}
	}
	return nil
}

func (s *socket) LeaveAllRooms() error {
	s.rmu.Lock()
	defer s.rmu.Unlock()

	for room, _ := range s.rooms {
		if err := s.adapter.Remove(room, s); err != nil {
			return err
		}
	}

	s.rooms = make(map[string]bool)

	return nil
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

func (s *socket) OnEvent(packet siop.Packet) error {
	args := s.unpackData(packet.Data)

	if packet.ID != nil {
		emitArgs := make([]any, len(args)+1)
		copy(emitArgs, args)
		emitArgs[len(args)] = ReceivedByLocalAcknowledgement(func(args1 ...any) error {
			ack := CreateDataPacket(siop.Ack, "", args1...)
			ack.ID = packet.ID
			return s.SendPacket(ack)
		})
		args = emitArgs
	}

	event := args[0].(string)
	eventArgs := make([]any, len(args)-1)
	copy(eventArgs, args[1:])

	if err := s.Emit(event, eventArgs...); err != nil {
		return err
	}

	s.emu.Lock()
	defer s.emu.Unlock()

	for _, listener := range s.allEventListeners {
		if err := listener(event, eventArgs...); err != nil {
			return err
		}
	}
	return nil
}

func (s *socket) OnAck(packet siop.Packet) error {
	s.amu.RLock()
	ack, ok := s.acknowledgements[*packet.ID]
	s.amu.RUnlock()

	if ok {
		s.amu.Lock()
		delete(s.acknowledgements, *packet.ID)
		s.amu.Unlock()

		return ack(s.unpackData(packet.Data)...)
	}
	return nil
}

func (s *socket) OnPacket(packet siop.Packet) error {
	switch packet.Type {
	case siop.Event, siop.BinaryEvent:
		return s.OnEvent(packet)
	case siop.Ack, siop.BinaryAck:
		return s.OnAck(packet)
	case siop.Disconnect:
		return s.OnDisconnect()
	case siop.ConnectError:
		return s.OnError(packet.Data.(string))
	}
	return nil
}

func (s *socket) OnConnect() error {
	s.log.Debug("sio.Socket new connection", zap.String("sid", s.GetSID()), zap.Any("connect_data", s.connectData))

	s.namespace.AddConnected(s)

	if err := s.JoinRooms(s.GetSID()); err != nil {
		return err
	}

	return s.SendPacket(siop.Packet{
		Type: siop.Connect,
		Data: map[string]string{
			"sid": s.GetSID(),
		},
	})
}

func (s *socket) OnDisconnect() error {
	return s.OnClose("client namespace disconnect")
}

func (s *socket) OnClose(reason string) error {
	if !s.connected {
		return nil
	}

	if err := s.Emit(eio.TopicDisconnecting, reason); err != nil {
		s.log.Warn("socket emit on close", zap.Error(err))
	}

	if err := s.LeaveAllRooms(); err != nil {
		s.log.Warn("socket leave rooms", zap.Error(err))
	}

	s.namespace.Remove(s)
	s.client.Remove(s)
	s.connected = false
	s.namespace.RemoveConnected(s)

	return s.Emit(eio.TopicDisconnect, reason)
}

func (s *socket) OnError(msg string) error {
	return s.Emit(eio.TopicError, msg)
}

func (s *socket) SendPacket(packet siop.Packet) error {
	packet.Nsp = s.namespace.GetName()
	return s.client.SendPacket(packet)
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
