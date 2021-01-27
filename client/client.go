package client

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"sonekKV/util/logutil"
)

var (
	ErrNoAvailableEndpoint = errors.New("no available endpoint")
)

type Client struct {
	KV
	Watcher
	conn *grpc.ClientConn

	cfg Config

	ctx context.Context
	cancel context.CancelFunc
	callOpts []grpc.CallOption

	lg *zap.Logger

}

func New(cfg Config) (*Client, error) {
	if cfg.Endpoint == "" {
		return nil, ErrNoAvailableEndpoint
	}

	return newClient(&cfg)
}

func newClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = &Config{}
	}

	baseCtx := context.Background()
	if cfg.Context != nil {
		baseCtx = cfg.Context
	}
	ctx, cancel := context.WithCancel(baseCtx)
	client := &Client{
		conn:     nil,
		cfg:      *cfg,
		ctx:      ctx,
		cancel:   cancel,
		callOpts: defaultCallOpts,
	}

	var lcfg = logutil.DefaultZapLoggerConfig
	if cfg.LogConfig != nil {
		lcfg = *cfg.LogConfig
	}
	var err error
	client.lg, err = lcfg.Build()
	if err != nil {
		return nil, err
	}
	if cfg.MaxCallSendMsgSize > 0 || cfg.MaxCallRecvMsgSize > 0 {
		if cfg.MaxCallRecvMsgSize > 0 && cfg.MaxCallSendMsgSize > cfg.MaxCallRecvMsgSize {
			return nil, fmt.Errorf("gRPC message recv limit (%d bytes) must be greater than send limit (%d bytes)", cfg.MaxCallRecvMsgSize, cfg.MaxCallSendMsgSize)
		}
		callOpts := []grpc.CallOption{
			defaultMaxCallSendMsgSize,
			defaultMaxCallRecvMsgSize,
		}
		if cfg.MaxCallSendMsgSize > 0 {
			callOpts[1] = grpc.MaxCallSendMsgSize(cfg.MaxCallSendMsgSize)
		}
		if cfg.MaxCallRecvMsgSize > 0 {
			callOpts[2] = grpc.MaxCallRecvMsgSize(cfg.MaxCallRecvMsgSize)
		}
		client.callOpts = callOpts
	}

	dialEndpoint := cfg.Endpoint

	var dopts []grpc.DialOption
	dopts = append(dopts, grpc.WithInsecure())

	conn, err := client.dial(dialEndpoint, dopts...)
	if err != nil {
		client.cancel()
		return nil, err
	}

	client.conn = conn
	client.KV = NewKV(client)
	client.Watcher = NewWatcher(client)

	return client, nil
}

func (c *Client) dial(ep string, dopts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts, err :=  c.dialSetupOpts(dopts...)
	if err != nil {
		return nil, fmt.Errorf("failed to configure dialer: %v", err)
	}
	dctx := c.ctx
	if c.cfg.DialTimeout > 0 {
		var cancel context.CancelFunc
		dctx, cancel = context.WithTimeout(c.ctx, c.cfg.DialTimeout)
		defer cancel()
	}
	conn, err := grpc.DialContext(dctx, ep, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create a connection to server: %v", err)
	}
	return conn, nil
}

func (c *Client) dialSetupOpts(dopts ...grpc.DialOption) (opts []grpc.DialOption, err error) {
	if c.cfg.DialKeepAliveTime > 0 {
		params := keepalive.ClientParameters{
			Time:                c.cfg.DialKeepAliveTime,
			Timeout:             c.cfg.DialKeepAliveTimeout,
			PermitWithoutStream: c.cfg.PermitWithoutStream,
		}
		opts = append(opts, grpc.WithKeepaliveParams(params))
	}
	opts = append(opts, dopts...)
	opts = append(opts, grpc.WithInsecure())

	return opts, nil
}


func isHaltErr(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	if err == nil {
		return false
	}
	ev, _ := status.FromError(err)
	return ev.Code() != codes.Unavailable && ev.Code() != codes.Internal
}

func (c *Client) Close() error {
	c.cancel()
	if c.Watcher != nil {
		return c.Watcher.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}


func isUnavailableErr(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if err == nil {
		return false
	}
	ev, ok := status.FromError(err)
	if ok {
		return ev.Code() == codes.Unavailable
	}
	return false
}




