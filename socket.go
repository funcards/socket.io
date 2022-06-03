package sio

import (
	"encoding/json"
	"errors"
	"github.com/funcards/engine.io"
	"github.com/funcards/socket.io-parser/v5"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"net/url"
	"sync"
)

var _ Socket = (*socket)(nil)

// ReceivedByRemoteAck callback for remote received acknowledgement. Called when remote client calls ack callback.
type ReceivedByRemoteAck func(args ...any)

// ReceivedByLocalAck callback for local received acknowledgement. Call this method to send ack to remote client.
type ReceivedByLocalAck func(args ...any)

type Socket interface {
	eio.Emitter

	SID() string
	Namespace() Namespace
	ConnectData() any
	Query() url.Values
	Headers() map[string]string
	Disconnect(close bool)
	Broadcast(rooms []string, event string, args ...any)
	Send(remoteAck ReceivedByRemoteAck, event string, args ...any)
	JoinRooms(rooms ...string)
	LeaveRooms(rooms ...string)
	LeaveAllRooms()
	OnEvent(packet siop.Packet)
	OnAck(packet siop.Packet)
	OnPacket(packet siop.Packet)
	OnConnect()
	OnDisconnect()
	OnError(msg string, err error)
	OnClose(reason, description string)
	SendPacket(packet siop.Packet)
}

type socket struct {
	eio.Emitter

	connected *atomic.Bool
	sid       string
	nsp       Namespace
	client    Client
	data      any
	log       *zap.Logger
	rooms     *sync.Map
	acks      *sync.Map
}

func NewSocket(nsp Namespace, client Client, data any, logger *zap.Logger) *socket {
	return &socket{
		Emitter:   eio.NewEmitter(),
		sid:       eio.NewSID(),
		connected: atomic.NewBool(true),
		nsp:       nsp,
		client:    client,
		data:      data,
		log:       logger,
		rooms:     new(sync.Map),
		acks:      new(sync.Map),
	}
}

func (s *socket) SID() string {
	return s.sid
}

func (s *socket) Namespace() Namespace {
	return s.nsp
}

func (s *socket) ConnectData() any {
	return s.data
}

func (s *socket) Query() url.Values {
	return s.client.Query()
}

func (s *socket) Headers() map[string]string {
	return s.client.Headers()
}

func (s *socket) Disconnect(close bool) {
	if s.connected.Load() {
		if close {
			s.client.Disconnect()
			return
		}
		s.SendPacket(siop.Packet{Type: siop.Disconnect})
		s.OnClose("server namespace disconnect", "")
	}
}

func (s *socket) Broadcast(rooms []string, event string, args ...any) {
	if len(event) == 0 {
		s.log.Warn("sio.Socket broadcast: event is empty")
		return
	}

	s.log.Debug("sio.socket broadcast message", zap.String("sid", s.sid))
	s.nsp.Adapter().Broadcast(CreateDataPacket(siop.Event, event, args...), rooms, s.sid)
}

func (s *socket) Send(remoteAck ReceivedByRemoteAck, event string, args ...any) {
	if len(event) == 0 {
		s.log.Warn("sio.Socket send: event is empty")
		return
	}

	packet := CreateDataPacket(siop.Event, event, args...)
	if remoteAck != nil {
		id := s.nsp.NextAckID()
		packet.ID = &id
		s.acks.Store(packet.ID, remoteAck)
	}
	s.SendPacket(packet)
}

func (s *socket) JoinRooms(rooms ...string) {
	for _, room := range rooms {
		if _, ok := s.rooms.LoadOrStore(room, true); !ok {
			s.nsp.Adapter().Add(room, s)
		}
	}
}

func (s *socket) LeaveRooms(rooms ...string) {
	for _, room := range rooms {
		if _, ok := s.rooms.LoadAndDelete(room); ok {
			s.nsp.Adapter().Remove(room, s)
		}
	}
}

func (s *socket) LeaveAllRooms() {
	s.rooms.Range(func(key, value any) bool {
		s.nsp.Adapter().Remove(key.(string), s)
		return true
	})
	s.rooms = new(sync.Map)
}

func (s *socket) OnEvent(packet siop.Packet) {
	args := s.unpackData(packet.Data)

	if packet.ID != nil {
		args = append(args, ReceivedByLocalAck(func(args1 ...any) {
			ack := CreateDataPacket(siop.Ack, "", args1...)
			ack.ID = packet.ID
			s.SendPacket(ack)
		}))
	}

	s.Fire(args[0].(string), args[1:]...)
}

func (s *socket) OnAck(packet siop.Packet) {
	if ack, ok := s.acks.LoadAndDelete(packet.ID); ok {
		ack.(ReceivedByRemoteAck)(s.unpackData(packet.Data)...)
	}
}

func (s *socket) OnPacket(packet siop.Packet) {
	switch packet.Type {
	case siop.Event, siop.BinaryEvent:
		s.OnEvent(packet)
	case siop.Ack, siop.BinaryAck:
		s.OnAck(packet)
	case siop.Disconnect:
		s.OnDisconnect()
	case siop.ConnectError:
		s.OnError(packet.Type.String(), errors.New(packet.Data.(string)))
	}
}

func (s *socket) OnConnect() {
	s.nsp.AddConnected(s)
	s.JoinRooms(s.sid)
	s.SendPacket(siop.Packet{
		Type: siop.Connect,
		Data: map[string]string{
			"sid": s.sid,
		},
	})
}

func (s *socket) OnDisconnect() {
	s.OnClose("client disconnect", "")
}

func (s *socket) OnError(msg string, err error) {
	s.log.Warn(msg, zap.Error(err))
	s.Fire(eio.TopicError, msg, err.Error())
}

func (s *socket) OnClose(reason, description string) {
	s.log.Debug("sio.Socket OnClose -- START", zap.String("sid", s.sid), zap.String("reason", reason), zap.String("description", description))

	if !s.connected.Load() {
		return
	}

	s.Fire(eio.TopicDisconnecting, reason, description)

	s.LeaveAllRooms()
	s.connected.Store(false)
	s.client.Remove(s)
	s.nsp.Remove(s)
	s.nsp.RemoveConnected(s)

	s.Fire(eio.TopicDisconnect, reason, description)

	s.log.Debug("sio.Socket OnClose -- END", zap.String("sid", s.sid), zap.String("reason", reason), zap.String("description", description))
}

func (s *socket) SendPacket(packet siop.Packet) {
	packet.Nsp = s.nsp.Name()
	s.client.Send(packet)
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
