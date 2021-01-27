package main

import (
	"flag"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"sonekKV/util/logutil"
	"time"
)

const (
	DefaultName = "Sonek-KV"
	DefaultDataDir = "data"

	DefaultMaxRequestBytes       = 1.5 * 1024 * 1024
	DefaultGRPCKeepAliveMinTime  = 5 * time.Second
	DefaultGRPCKeepAliveInterval = 2 * time.Hour
	DefaultGRPCKeepAliveTimeout  = 20 * time.Second

	DefaultHost = "0.0.0.0"
	DefaultPort = "2379"

	DefaultLogOutput = "logs/logger"

	EnvDevelopment = "dev"
	EnvProduction = "product"

)

type Config struct {
	Name string `json:"name"`
	Dir	string `json:"data-dir"`

	MaxRequestBytes uint `json:"max-request-bytes"`

	GRPCKeepAliveMinTime time.Duration `json:"grpc-keepalive-min-time"`
	GRPCKeepAliveInterval time.Duration `json:"grpc-keepalive-interval"`
	GRPCKeepAliveTimeout time.Duration `json:"grpc-keepalive-timeout"`

	Host, Port string

	loggerConfig *zap.Config
	logger *zap.Logger
	LogOutput string `json:"log-outputs"`

}

// configYAML holds the config suitable for yaml parsing
type configYAML struct {
	Config
	configJSON
}

// configJSON has file options that are translated into Config options
type configJSON struct {
	Host string `json:"host"`
	Port string `json:"port"`
	
	DataDir string `json:"data-dir"`
	LogDir	string `json:"log-dir"`
}

func NewConfig() *Config  {
	cfg := &Config{
		Name: DefaultName,
		Dir: DefaultDataDir,

		MaxRequestBytes: DefaultMaxRequestBytes,

		GRPCKeepAliveInterval: DefaultGRPCKeepAliveInterval,
		GRPCKeepAliveMinTime: DefaultGRPCKeepAliveMinTime,
		GRPCKeepAliveTimeout: DefaultGRPCKeepAliveTimeout,

		Host: DefaultHost,
		Port: DefaultPort,

	}
	return cfg
}

func ConfigFromFile(path string) (*Config, error) {
	cfg := &configYAML{Config: *NewConfig()}
	if path != "" {
		if err := cfg.configFromFile(path); err != nil {
			return nil, err
		}
	}
	if err := cfg.setupLogging(); err != nil {
		return nil, err
	}
	return &cfg.Config, nil
}

func (cfg *configYAML) configFromFile(path string) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(b, cfg)
	if err != nil {
		return err
	}
	return nil
}

func (cfg *Config) setupLogging() error {
	var env = ""
	flag.StringVar(&env, "env", "dev", "environment [dev, product]")
	flag.Parse()
	if cfg.LogOutput == "" {
		cfg.LogOutput = DefaultLogOutput
	}
	copied := logutil.DefaultZapLoggerConfig
	if env == EnvProduction {
		copied.OutputPaths = append(copied.OutputPaths, cfg.LogOutput)
		copied.ErrorOutputPaths = append(copied.ErrorOutputPaths, cfg.LogOutput)
		copied.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
		copied.Encoding = "json"
	}

	var err error
	cfg.logger, err = copied.Build()
	if err != nil {
		return err
	}
	cfg.loggerConfig = &copied
	return nil
}