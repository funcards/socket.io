package sio

import (
	"context"
	"github.com/funcards/engine.io"
	"go.uber.org/zap"
	"sync"
	"time"
)

type (
	Config struct {
		ConnectionTimeout time.Duration `yaml:"connection_timeout" env-default:"90s" env:"SIO_CONNECTION_TIMEOUT"`
	}

	Server interface {
		eio.Emitter

		GetConfig() Config
		GetAdapterFactory() AdapterFactory
		HasNamespace(nsp string) bool
		CheckNamespace(nsp string) bool
		Namespace(nsp string) Namespace
	}

	server struct {
		eio.Emitter

		eServer        eio.Server
		adapterFactory AdapterFactory
		cfg            Config
		log            *zap.Logger

		namespaces map[string]*NamespaceImpl
		nmu        sync.RWMutex
	}
)

func NewServer(eServer eio.Server, adapterFactory AdapterFactory, cfg Config, logger *zap.Logger) *server {
	s := &server{
		Emitter:        eio.NewEmitter(logger),
		eServer:        eServer,
		adapterFactory: adapterFactory,
		cfg:            cfg,
		log:            logger,
		namespaces:     make(map[string]*NamespaceImpl),
	}

	_ = s.Namespace("/")

	eServer.On(eio.TopicConnection, func(ctx context.Context, event *eio.Event) {
		sck := event.Get(0).(eio.Socket)
		NewClient(ctx, s, sck, logger)
	})

	return s
}

func (s *server) GetConfig() Config {
	return s.cfg
}

func (s *server) GetAdapterFactory() AdapterFactory {
	return s.adapterFactory
}

func (s *server) HasNamespace(nsp string) bool {
	if nsp[0] != '/' {
		nsp = "/" + nsp
	}

	s.nmu.RLock()
	defer s.nmu.RUnlock()

	_, ok := s.namespaces[nsp]

	return ok
}

func (s *server) CheckNamespace(nsp string) bool {
	if nsp[0] != '/' {
		nsp = "/" + nsp
	}
	return false // TODO: implements provider functionality
}

func (s *server) Namespace(nsp string) Namespace {
	if nsp[0] != '/' {
		nsp = "/" + nsp
	}

	s.nmu.RLock()
	n, ok := s.namespaces[nsp]
	s.nmu.RUnlock()

	if !ok {
		n = NewNamespaceImpl(nsp, s, s.log)

		s.nmu.Lock()
		s.namespaces[nsp] = n
		s.nmu.Unlock()
	}

	return n
}
