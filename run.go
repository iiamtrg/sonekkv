package main

import (
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"runtime"
	"sonekKV/kvserver"
	"sonekKV/kvserver/api/rpc"
	"strings"
)

const fileName = "config.yaml"

func startServer()  {
	grpc.EnableTracing = true
	cfg, err := ConfigFromFile("")
	if err != nil {
		panic("failed to read config")
	}

	srvCfg := kvserver.ServerConfig{
		Name: cfg.Name,
		Host: cfg.Host,
		Port: cfg.Port,
		DataDir: cfg.Dir,
		MaxRequestBytes: cfg.MaxRequestBytes,
		Logger: cfg.logger,
	}
	lg := srvCfg.Logger
	lg.Info(
		"starting an server",
		zap.String("go-os", runtime.GOOS),
		zap.String("go-arch", runtime.GOARCH),
		zap.Int("max-cpu-set", runtime.GOMAXPROCS(0)),
		zap.Int("max-cpu-available", runtime.NumCPU()),
		zap.String("name", srvCfg.Name),
		)

	s, err := kvserver.NewServer(srvCfg)
	if err != nil {
		lg.Fatal("failed to create new an server")
		return
	}
	listener, err := net.Listen("tcp", strings.Join([]string{s.Cfg.Host, s.Cfg.Port}, ":"))
	if err != nil {
		lg.Fatal("failed to create new a listener ", zap.Error(err))
		return
	}
	gs := rpc.Server(s)
	lg.Info("server start listening on address ", zap.String("host", srvCfg.Host), zap.String("port", srvCfg.Port))
	if err = gs.Serve(listener); err != nil {
		lg.Fatal("failed to start server", zap.Error(err))
		return
	}

}

func Start()  {
	startServer()
}
