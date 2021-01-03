package schema_registry

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
)

type LogLevel uint32

const LogNothing LogLevel = 0
const LogWarnings LogLevel = 1
const LogHttp LogLevel = 2
const LogCaches LogLevel = 4
const LogEntities = 128
const LogEverything LogLevel = 255

type Config struct {
	LogLevel LogLevel
	Tls      *tls.Config
	Url      string
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

func (c *Config) LogEntities() bool {
	return c.LogLevel & LogEntities > 0
}

func (c *Config) AddCaCert(caPemFile string) error {
	if c.Tls == nil {
		c.Tls = new(tls.Config)
	}
	if c.Tls.RootCAs == nil {
		c.Tls.RootCAs = x509.NewCertPool()
	}
	caCert, err := ioutil.ReadFile(caPemFile)
	if err != nil {
		return err
	}
	if ok := c.Tls.RootCAs.AppendCertsFromPEM(caCert); !ok {
		return fmt.Errorf("could not add root ca from %v", caPemFile)
	}
	return nil
}

func (c *Config) AddClientCert(certFile, keyFile, keyPass string) error {

	if c.Tls == nil {
		c.Tls = new(tls.Config)
	}
	var cert tls.Certificate
	certBlock, err := ioutil.ReadFile(certFile)
	if err != nil {
		return err
	}
	pemData, err := ioutil.ReadFile(keyFile)
	if err != nil {
		return err
	}
	v, _ := pem.Decode(pemData)
	if v == nil {
		return fmt.Errorf("no RAS key found in file: %v", keyFile)
	}
	if v.Type == "RSA PRIVATE KEY" {
		var pkey []byte
		if x509.IsEncryptedPEMBlock(v) {
			pkey, _ = x509.DecryptPEMBlock(v, []byte(keyPass))
			pkey = pem.EncodeToMemory(&pem.Block{
				Type:  v.Type,
				Bytes: pkey,
			})
		} else {
			pkey = pem.EncodeToMemory(v)
		}
		if cert, err = tls.X509KeyPair(certBlock, pkey); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("only RSA private keys in PEM form are supported, got: %q", v.Type)
	}
	c.Tls.Certificates = []tls.Certificate{cert}

	return nil
}
