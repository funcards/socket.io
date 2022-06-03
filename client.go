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

type Client interface {
	SID() string
	Query() url.Values
	Headers() map[string]string
	Conn() eio.Socket
	Send(packet siop.Packet)
	Remove(sck Socket)
	Disconnect()
}

type client struct {
	srv           Server
	conn          eio.Socket
	log           *zap.Logger
	decoder       siop.Decoder
	mu            sync.Mutex
	timeoutFuture *time.Timer
	sockets       *sync.Map
	nspSockets    *sync.Map
	stop          chan struct{}
}

func NewClient(srv Server, conn eio.Socket, logger *zap.Logger) *client {
	c := &client{
		srv:        srv,
		conn:       conn,
		log:        logger,
		decoder:    siop.NewDecoder(nil),
		sockets:    new(sync.Map),
		nspSockets: new(sync.Map),
		stop:       make(chan struct{}, 1),
	}
	c.setup()
	return c
}

func (c *client) SID() string {
	return c.conn.SID()
}

func (c *client) Query() url.Values {
	return c.conn.Query()
}

func (c *client) Headers() map[string]string {
	return c.conn.Headers()
}

func (c *client) Conn() eio.Socket {
	return c.conn
}

func (c *client) Send(packet siop.Packet) {
	if c.conn.State() == eio.Open {
		for _, encoded := range packet.Encode() {
			c.conn.Send(eiop.MessagePacket(encoded))
		}
	}
}

func (c *client) Remove(sck Socket) {
	c.sockets.Delete(sck.SID())
	c.nspSockets.Delete(sck.Namespace().Name())
}

func (c *client) Disconnect() {
	c.sockets.Range(func(key, value any) bool {
		value.(Socket).Disconnect(false)
		c.sockets.Delete(key)
		return true
	})
	c.close()
}

func (c *client) setup() {
	c.decoder.OnDecoded(func(packet siop.Packet) error {
		if packet.Type == siop.Connect {
			c.connect(packet.Nsp, packet.Data)
		} else if value, ok := c.nspSockets.Load(packet.Nsp); ok {
			value.(Socket).OnPacket(packet)
		}
		return nil
	})

	go c.run()

	c.mu.Lock()
	defer c.mu.Unlock()
	c.timeoutFuture = time.AfterFunc(c.srv.Cfg().ConnectionTimeout, c.connectionTimeout)
}

func (c *client) run() {
	onData := c.conn.On(eio.TopicData)
	onClose := c.conn.Once(eio.TopicClose)

	defer func(conn eio.Socket) {
		conn.Off(eio.TopicClose, onClose)
		conn.Off(eio.TopicData, onData)
	}(c.conn)

	for {
		select {
		case <-c.stop:
			return
		case event := <-onClose:
			c.onClose(event.String(1), event.String(2))
			return
		case event := <-onData:
			if err := c.decoder.Add(event.Args[1]); err != nil {
				c.onError("decode data", err)
			}
		}
	}
}

func (c *client) connect(nsp string, data any) {
	if c.srv.HasNamespace(nsp) {
		c.doConnect(nsp, data)
		return
	}
	c.Send(siop.Packet{
		Type: siop.ConnectError,
		Nsp:  nsp,
		Data: map[string]string{
			"message": "Invalid namespace",
		},
	})
}

func (c *client) doConnect(nsp string, data any) {
	n := c.srv.Namespace(nsp)
	sck := n.Add(c, data)
	c.sockets.Store(sck.SID(), sck)
	c.nspSockets.Store(nsp, sck)
}

func (c *client) close() {
	if c.conn.State() == eio.Open {
		c.onClose("forced server close", "")
	}
}

func (c *client) onError(msg string, err error) {
	c.log.Warn(msg, zap.Error(err))
	c.onClose(msg, err.Error())
}

func (c *client) onClose(reason, description string) {
	c.log.Debug("sio.Client on close", zap.String("reason", reason), zap.String("description", description))
	c.stopTimeout()
	close(c.stop)
	_ = c.conn.Close()
	c.sockets.Range(func(key, value any) bool {
		value.(Socket).OnClose(reason, description)
		return true
	})
	c.decoder.Destroy()
	c.conn = nil
	c.srv = nil
}

func (c *client) connectionTimeout() {
	timeout := true
	c.nspSockets.Range(func(any, any) bool {
		timeout = false
		return false
	})
	if timeout {
		c.close()
	}
}

func (c *client) stopTimeout() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.timeoutFuture != nil {
		if !c.timeoutFuture.Stop() {
			<-c.timeoutFuture.C
		}
	}
}
