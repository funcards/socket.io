package sio

import (
	"github.com/funcards/engine.io"
	"github.com/funcards/engine.io-parser/v4"
	"github.com/funcards/socket.io-parser/v5"
	"go.uber.org/zap"
	"net/url"
	"sync"
	"time"
)

var _ Client = (*client)(nil)

type (
	Client interface {
		GetSID() string
		GetInitialQuery() url.Values
		GetInitialHeaders() map[string]string
		GetConnection() eio.Socket
		SendPacket(packet siop.Packet) error
		Remove(sck Socket)
		Connect(nsp string, data any) error
		Disconnect() error
	}

	client struct {
		server        Server
		connection    eio.Socket
		log           *zap.Logger
		decoder       siop.Decoder
		timeoutFuture *time.Timer

		sockets map[string]Socket
		smu     sync.RWMutex

		namespaceSockets map[string]Socket
		nmu              sync.RWMutex
	}
)

func NewClient(server Server, connection eio.Socket, logger *zap.Logger) *client {
	c := &client{
		server:           server,
		connection:       connection,
		log:              logger,
		decoder:          siop.NewDecoder(nil),
		sockets:          make(map[string]Socket),
		namespaceSockets: make(map[string]Socket),
	}
	c.setup()

	return c
}

func (c *client) GetSID() string {
	return c.connection.GetSID()
}

func (c *client) GetInitialQuery() url.Values {
	return c.connection.GetInitialQuery()
}

func (c *client) GetInitialHeaders() map[string]string {
	return c.connection.GetInitialHeaders()
}

func (c *client) GetConnection() eio.Socket {
	return c.connection
}

func (c *client) SendPacket(packet siop.Packet) error {
	if c.connection.GetState() == eio.Open {
		for _, encoded := range packet.Encode() {
			ePck := eiop.MessagePacket(encoded)
			if err := c.connection.Send(ePck); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *client) Remove(sck Socket) {
	c.smu.RLock()
	_, ok := c.sockets[sck.GetSID()]
	c.smu.RUnlock()

	if ok {
		c.smu.Lock()
		delete(c.sockets, sck.GetSID())
		c.smu.Unlock()

		c.nmu.Lock()
		delete(c.namespaceSockets, sck.GetNamespace().GetName())
		c.nmu.Unlock()
	}
}

func (c *client) Connect(nsp string, data any) error {
	if c.server.HasNamespace(nsp) || c.server.CheckNamespace(nsp) {
		return c.doConnect(nsp, data)
	}

	return c.SendPacket(siop.Packet{
		Type: siop.ConnectError,
		Nsp:  nsp,
		Data: map[string]string{
			"message": "Invalid namespace",
		},
	})
}

func (c *client) Disconnect() error {
	c.smu.Lock()
	for sid, sck := range c.sockets {
		if err := sck.Disconnect(false); err != nil {
			c.log.Warn("socket client disconnect", zap.Error(err))
		}
		delete(c.sockets, sid)
	}
	c.smu.Unlock()

	return c.close()
}

func (c *client) close() error {
	if c.connection.GetState() == eio.Open {
		if err := c.connection.Close(); err != nil {
			c.log.Warn("socket client close", zap.Error(err))
		}
		return c.onClose("forced server close")
	}
	return nil
}

func (c *client) setup() {
	c.decoder.OnDecoded(func(pkt siop.Packet) error {
		if pkt.Type == siop.Connect {
			return c.Connect(pkt.Nsp, pkt.Data)
		}

		c.nmu.RLock()
		defer c.nmu.RUnlock()

		if sck, ok := c.namespaceSockets[pkt.Nsp]; ok {
			return sck.OnPacket(pkt)
		}

		return nil
	})
	c.connection.On(eio.TopicData, func(event *eio.Event) error {
		if err := c.decoder.Add(event.Get(1)); err != nil {
			return c.onError(err.Error())
		}
		return nil
	})
	c.connection.On(eio.TopicError, func(event *eio.Event) error {
		return c.onError(event.String(1))
	})
	c.connection.On(eio.TopicClose, func(event *eio.Event) error {
		return c.onClose(event.String(1))
	})

	c.timeoutFuture = time.AfterFunc(c.server.GetConfig().ConnectionTimeout, c.connectionTimeout)
}

func (c *client) destroy() {
	c.connection.Off(eio.TopicData, eio.TopicError, eio.TopicClose)
}

func (c *client) doConnect(nsp string, data any) error {
	nspImpl := c.server.Namespace(nsp).(*NamespaceImpl)
	sck, err := nspImpl.Add(c, data)
	if err != nil {
		return err
	}

	c.smu.Lock()
	c.sockets[sck.GetSID()] = sck
	c.smu.Unlock()

	c.nmu.Lock()
	c.namespaceSockets[nsp] = sck
	c.nmu.Unlock()

	return nil
}

func (c *client) onClose(reason string) error {
	c.stopTimers()
	c.destroy()

	c.smu.Lock()
	for sid, sck := range c.sockets {
		if err := sck.OnClose(reason); err != nil {
			c.log.Warn("socket client onCLose", zap.Error(err))
			delete(c.sockets, sid)
		}
	}
	c.smu.Unlock()

	c.decoder.Destroy()

	return nil
}

func (c *client) onError(msg string) error {
	c.smu.RLock()
	defer c.smu.RUnlock()

	for _, sck := range c.sockets {
		if err := sck.OnError(msg); err != nil {
			c.log.Warn("socket client onError", zap.Error(err))
		}
	}

	return c.connection.Close()
}

func (c *client) connectionTimeout() {
	c.nmu.RLock()
	defer c.nmu.RUnlock()

	if len(c.namespaceSockets) == 0 {
		if err := c.close(); err != nil {
			c.log.Warn("socket connection timeout", zap.Error(err))
		}
	}
}

func (c *client) stopTimers() {
	if c.timeoutFuture != nil {
		c.timeoutFuture.Stop()
	}
}
