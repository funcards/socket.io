package sio

import (
	"context"
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
		SendPacket(ctx context.Context, packet siop.Packet)
		Remove(sck Socket)
		Connect(ctx context.Context, nsp string, data any)
		Disconnect(ctx context.Context)
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

func NewClient(ctx context.Context, server Server, connection eio.Socket, logger *zap.Logger) *client {
	c := &client{
		server:           server,
		connection:       connection,
		log:              logger,
		decoder:          siop.NewDecoder(nil),
		sockets:          make(map[string]Socket),
		namespaceSockets: make(map[string]Socket),
	}
	c.setup(ctx)

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

func (c *client) SendPacket(ctx context.Context, packet siop.Packet) {
	if c.connection.GetState() == eio.Open {
		for _, encoded := range packet.Encode() {
			ePck := eiop.MessagePacket(encoded)
			c.connection.Send(ctx, ePck)
		}
	}
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

func (c *client) Connect(ctx context.Context, nsp string, data any) {
	if c.server.HasNamespace(nsp) || c.server.CheckNamespace(nsp) {
		c.doConnect(ctx, nsp, data)
		return
	}

	c.SendPacket(ctx, siop.Packet{
		Type: siop.ConnectError,
		Nsp:  nsp,
		Data: map[string]string{
			"message": "Invalid namespace",
		},
	})
}

func (c *client) Disconnect(ctx context.Context) {
	c.smu.Lock()
	for sid, sck := range c.sockets {
		sck.Disconnect(ctx, false)
		delete(c.sockets, sid)
	}
	c.smu.Unlock()

	c.close(ctx)
}

func (c *client) close(ctx context.Context) {
	if c.connection.GetState() == eio.Open {
		c.connection.Close(ctx)
		c.onClose(ctx, "forced server close")
	}
}

func (c *client) setup(ctx context.Context) {
	c.decoder.OnDecoded(func(pkt siop.Packet) error {
		if pkt.Type == siop.Connect {
			c.Connect(ctx, pkt.Nsp, pkt.Data)
			return nil
		}

		c.nmu.RLock()
		defer c.nmu.RUnlock()

		if sck, ok := c.namespaceSockets[pkt.Nsp]; ok {
			sck.OnPacket(ctx, pkt)
		}

		return nil
	})
	c.connection.On(eio.TopicData, func(ctx context.Context, event *eio.Event) {
		if err := c.decoder.Add(event.Get(1)); err != nil {
			c.onError(ctx, err.Error())
		}
	})
	c.connection.On(eio.TopicError, func(ctx context.Context, event *eio.Event) {
		c.onError(ctx, event.String(1))
	})
	c.connection.On(eio.TopicClose, func(ctx context.Context, event *eio.Event) {
		c.onClose(ctx, event.String(1))
	})

	c.timeoutFuture = time.AfterFunc(c.server.GetConfig().ConnectionTimeout, c.connectionTimeout)
}

func (c *client) destroy() {
	c.connection.Off(eio.TopicData, eio.TopicError, eio.TopicClose)
}

func (c *client) doConnect(ctx context.Context, nsp string, data any) {
	nspImpl := c.server.Namespace(nsp).(*NamespaceImpl)
	sck := nspImpl.Add(ctx, c, data)

	c.smu.Lock()
	c.sockets[sck.GetSID()] = sck
	c.smu.Unlock()

	c.nmu.Lock()
	c.namespaceSockets[nsp] = sck
	c.nmu.Unlock()
}

func (c *client) onClose(ctx context.Context, reason string) {
	c.stopTimers()
	c.destroy()

	c.smu.Lock()
	for _, sck := range c.sockets {
		sck.OnClose(ctx, reason)
	}
	c.smu.Unlock()

	c.decoder.Destroy()
}

func (c *client) onError(ctx context.Context, msg string) {
	c.smu.RLock()
	defer c.smu.RUnlock()

	for _, sck := range c.sockets {
		sck.OnError(ctx, msg)
	}

	c.connection.Close(ctx)
}

func (c *client) connectionTimeout() {
	c.nmu.RLock()
	defer c.nmu.RUnlock()

	if len(c.namespaceSockets) == 0 {
		c.close(context.Background())
	}
}

func (c *client) stopTimers() {
	if c.timeoutFuture != nil {
		c.timeoutFuture.Stop()
	}
}
