package schema_registry

import "crypto/tls"

type LogLevel uint32

const LogNothing LogLevel = 0
const LogWarnings LogLevel = 1
const LogHttp LogLevel = 2
const LogCaches LogLevel = 4
const LogEverything LogLevel = 255

type Config struct {
	LogLevel LogLevel
	Tls *tls.Config
}

func (c *Config) LogWarnings() bool {
	return c.LogLevel & LogWarnings > 0
}

func (c *Config) LogHttp() bool {
	return c.LogLevel & LogHttp > 0
}

func (c *Config) LogCaches() bool {
	return c.LogLevel & LogCaches > 0
}