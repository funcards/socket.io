package sio

import (
	"github.com/funcards/engine.io"
	"go.uber.org/zap"
	"sync"
	"time"
)

var _ Server = (*server)(nil)

type Config struct {
	ConnectionTimeout time.Duration `yaml:"connection_timeout" env-default:"90s" env:"SIO_CONNECTION_TIMEOUT"`
}

type Server interface {
	Cfg() Config
	Shutdown()
	AdapterFactory() AdapterFactory
	HasNamespace(nsp string) bool
	Namespace(nsp string) Namespace
}

type server struct {
	cfg            Config
	eServer        eio.Server
	adapterFactory AdapterFactory
	log            *zap.Logger
	namespaces     *sync.Map
	stop           chan struct{}
}

func NewServer(cfg Config, eServer eio.Server, adapterFactory AdapterFactory, logger *zap.Logger) *server {
	s := &server{
		cfg:            cfg,
		eServer:        eServer,
		adapterFactory: adapterFactory,
		log:            logger,
		namespaces:     new(sync.Map),
		stop:           make(chan struct{}, 1),
	}
	s.setup()
	return s
}

func (s *server) Cfg() Config {
	return s.cfg
}

func (s *server) Shutdown() {
	close(s.stop)
}

func (s *server) AdapterFactory() AdapterFactory {
	return s.adapterFactory
}

func (s *server) HasNamespace(nsp string) bool {
	_, ok := s.namespaces.Load(norm(nsp))
	return ok
}

func (s *server) Namespace(nsp string) Namespace {
	nsp = norm(nsp)
	if value, ok := s.namespaces.Load(nsp); ok {
		return value.(Namespace)
	}

	s.log.Debug("create new namespace", zap.String("nsp", nsp))
	n := NewNamespace(nsp, s, s.log)
	s.namespaces.Store(nsp, n)
	return n
}

func (s *server) setup() {
	_ = s.Namespace("/")
	go s.run()
}

func (s *server) run() {
	onConnection := s.eServer.On(eio.TopicConnection)
	defer s.eServer.Off(eio.TopicConnection, onConnection)

	for {
		select {
		case <-s.stop:
			return
		case event := <-onConnection:
			sck := event.Args[0].(eio.Socket)
			NewClient(s, sck, s.log)
		}
	}
}

func norm(nsp string) string {
	if nsp[0] != '/' {
		return "/" + nsp
	}
	return nsp
}
