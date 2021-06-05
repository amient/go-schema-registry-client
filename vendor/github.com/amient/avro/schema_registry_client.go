package avro

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"time"
)

//SchemaRegistryClient is not concurrent //TODO maybe if both caches were sync.Map{} then it would become safe

type SchemaRegistryClient struct {
	Url    string
	Tls    *tls.Config
	cache1 map[uint32]Schema
	cache2 map[string]map[Fingerprint]uint32
}

type schemaResponse struct {
	Schema string
}

func (c *SchemaRegistryClient) Get(schemaId uint32) (Schema, error) {
	if c.cache1 == nil {
		c.cache1 = make(map[uint32]Schema)
	}
	result := c.cache1[schemaId]
	if result == nil {
		httpClient, err := c.getHttpClient()
		if err != nil {
			return nil, err
		}

		var url = c.Url + "/schemas/ids/" + strconv.Itoa(int(schemaId))
		ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)
		req, _ := http.NewRequest("GET", url, nil)
		resp, err := httpClient.Do(req.WithContext(ctx))
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("unexpected response from the schema registry: %v", resp.StatusCode)
		}
		if body, err := ioutil.ReadAll(resp.Body); err != nil {
			return nil, err
		} else {
			response := new(schemaResponse)
			json.Unmarshal(body, response)
			if result, err = ParseSchema(response.Schema); err != nil {
				return nil, err
			} else {
				c.cache1[schemaId] = result
			}
		}

	}
	return result, nil
}

func (c *SchemaRegistryClient) GetSchemaId(schema Schema, subject string) (uint32, error) {
	if c.cache2 == nil {
		c.cache2 = make(map[string]map[Fingerprint]uint32)
	}
	var s map[Fingerprint]uint32
	if s = c.cache2[subject]; s == nil {
		s = make(map[Fingerprint]uint32)
		c.cache2[subject] = s
	}

	if f, err := schema.Fingerprint(); err != nil {
		return 0, err
	} else {
		result, ok := s[*f]
		if !ok {
			request := make(map[string]string)
			request["schema"] = schema.String()
			if schemaJson, err := json.Marshal(request); err != nil {
				return 0, err
			} else if httpClient, err := c.getHttpClient(); err != nil {
				return 0, err
			} else {
				log.Printf("Registering schema for subject %q schema: %v", subject, schema.GetName())
				var url = c.Url + "/subjects/" + subject + "/versions"
				j := make(map[string]uint32)
				req, _ := http.NewRequest("POST", url, bytes.NewReader(schemaJson))
				req.Header.Set("Content-Type", "application/json")
				if resp, err := httpClient.Do(req); err != nil {
					return 0, err
				} else if resp.StatusCode != 200 {
					return 0, fmt.Errorf(resp.Status)
				} else if data, err := ioutil.ReadAll(resp.Body); err != nil {
					return 0, err
				} else if err := json.Unmarshal(data, &j); err != nil {
					return 0, err
				} else {
					result = j["id"]
					log.Printf("Got Schema ID: %v", result)
					s[*f] = result
				}
			}
		}
		return result, nil
	}

}

func (c *SchemaRegistryClient) getHttpClient() (*http.Client, error) {
	transport := new(http.Transport)
	transport.TLSClientConfig = c.Tls
	return &http.Client{Transport: transport}, nil
}

func (c *SchemaRegistryClient) Decode(bytes []byte, result interface{}, readerSchema Schema) (interface{}, error) {
	if result != nil {
		if reflect.ValueOf(result).Kind() != reflect.Ptr {
			return nil, fmt.Errorf("a non-reference type passed as into argument")
		} else if reflect.TypeOf(result).Elem().Kind() != reflect.Struct {
			return nil, fmt.Errorf("only struct references can be used as a result argument")
		}
	}

	switch bytes[0] {
	case 0:
		schemaId := binary.BigEndian.Uint32(bytes[1:])
		schema, err := c.Get(schemaId)
		if err != nil {
			return nil, err
		}
		if schema.Type() != Record && result != nil {
			return nil, fmt.Errorf("only record schemas can be mapped to result by reference")
		}
		payload := bytes[5:]
		var decoder = NewBinaryDecoder(payload)
		if readerSchema != nil {
			if result == nil {
				result = NewGenericRecord(schema)
			}
			reader, err := NewDatumProjector(readerSchema, schema)
			if err != nil {
				return nil, err
			}
			if err := reader.Read(result, decoder); err != nil {
				return nil, err
			}
			return result, nil
		} else {
			switch schema.Type() {
			case Boolean:
				if b, err := decoder.ReadBoolean(); err != nil {
					return nil, err
				} else {
					return b, nil
				}
			case Int:
				if i, err := decoder.ReadInt(); err != nil {
					return nil, err
				} else {
					return i, nil
				}
			case Enum:
				return nil, fmt.Errorf("enum is not supported as a top-level structure")

			case Array:
				return nil, fmt.Errorf("array is not supported as a top-level structure")

			case Map:
				return nil, fmt.Errorf("map is not supported as a top-level structure")

			case Union:
				return nil, fmt.Errorf("union is not supported as a top-level structure")

			case Fixed:
				b := make([]byte, schema.(*FixedSchema).Size)
				if err := decoder.ReadFixed(b); err != nil {
					return nil, err
				} else {
					return b, nil
				}

			case String:
				if s, err := decoder.ReadString(); err != nil {
					return nil, err
				} else {
					return s, nil
				}

			case Bytes:
				if b, err := decoder.ReadBytes(); err != nil {
					return nil, err
				} else {
					return b, nil
				}

			case Float:
				if n, err := decoder.ReadFloat(); err != nil {
					return nil, err
				} else {
					return n, nil
				}
			case Double:
				if n, err := decoder.ReadDouble(); err != nil {
					return nil, err
				} else {
					return n, nil
				}
			case Null:
				if n, err := decoder.ReadNull(); err != nil {
					return nil, err
				} else {
					return n, nil
				}
			case Long:
				if i64, err := decoder.ReadLong(); err != nil {
					return nil, err
				} else {
					return i64, nil
				}
			case Recursive:
				if result == nil {
					result = NewGenericRecord(schema)
				}
				reader := NewDatumReader(schema.(*RecursiveSchema).Actual)
				if err := reader.Read(result, decoder); err != nil {
					return nil, err
				} else {
					return result, nil
				}
			case Record:
				if result == nil {
					result = NewGenericRecord(schema)
				}
				reader := NewDatumReader(schema)
				if err := reader.Read(result, decoder); err != nil {
					return nil, err
				} else {
					return result, nil
				}
			default:
				return nil, fmt.Errorf("not implemented avro decode type: %v", schema.Type())
			}

		}
	default:
		return nil, errors.New("avro binary header incorrect")
	}
}

func TlsConfigFromPEM(certFile, keyFile, keyPass, caFile string) (*tls.Config, error) {
	config := new(tls.Config)

	var cert tls.Certificate

	if certFile != "" {
		if certBlock, err := ioutil.ReadFile(certFile); err != nil {
			return nil, err
		} else if pemData, err := ioutil.ReadFile(keyFile); err != nil {
			return nil, err
		} else if v, _ := pem.Decode(pemData); v == nil {
			return nil, fmt.Errorf("no RAS key found in file: %v", keyFile)
		} else if v.Type == "RSA PRIVATE KEY" {
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
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("only RSA private keys in PEM form are supported, got: %q", v.Type)
		}
		config.Certificates = []tls.Certificate{cert}
	}

	if caFile != "" {
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
			return nil, fmt.Errorf("could not add root ca from %v", caFile)
		}
		config.RootCAs = caCertPool
	}

	config.BuildNameToCertificate()
	return config, nil
}
