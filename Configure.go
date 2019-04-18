/*
 * Copyright (c) 2013-2014 by Farsight Security, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dnstap

import (
    "fmt"
    "log"
    "errors"
    "os"
    "time"

    "github.com/BurntSushi/toml"
)

type Config struct {
    Input       InputConfig
    OutputFile  OutputConfig
    OutputKafka KafkaConfig
}

type InputConfig struct {
    Type    string   `toml:"type"`
    Path    string   `toml:"path"`
}

type OutputConfig struct {
    Format string   `toml:"format"`
    Path   string   `toml:"path"`
    Append bool     `toml:"append"`
}

type KafkaConfig struct {
    Brokers     []string    `toml:"brokers"`
    Topic       string      `toml:"topic"`
    Key         string      `toml:"key"`
    Sasl        bool        `toml:"sasl"`
    User        string      `toml:"user"`
    Password    string      `toml:"password"`
    Acks        int16       `toml:"acks"`
    Compression int8        `toml:"compression"`
    Max_send_retries    int `toml:"max_send\retries"`
    Flush_interval      time.Duration `toml:"flush_interval"`
    Debug       bool        `toml:"debug"`
}

func (c *Config) validate() error {
    if c.Input.Type == "" {
            return errors.New("dnstap: Error: no inputs specified.\n")
    } else if c.OutputFile.Append {
        if c.OutputFile.Path == "-" ||  c.OutputFile.Path == "" {
            return errors.New("dnstap: Error: -a must specify the file output path.\n")
        }
    } else if c.OutputKafka.Brokers != nil {
        if c.OutputFile.Format != "" || c.OutputFile.Path != "" {
            return errors.New("dnstap: Error: outputs exactly one of file or kafka.\n")
        } else if c.OutputKafka.Topic == "" {
            return errors.New("dnstap: Error: outputs OutputKafka.Topic is requerd.\n")
        }
        if c.OutputKafka.Sasl {
            if c.OutputKafka.User == "" || c.OutputKafka.Password == ""{
                return errors.New("dnstap: Error: outputs OutputKafka.User|Password is requerd, if sasl is true.\n")
            }
        }

        switch c.OutputKafka.Acks {
        case 0,1,-1:
        default:
            return errors.New("dnstap: Error: outputs OutputKafka.Acks is 0,1 or -1.\n")
        }

        switch c.OutputKafka.Compression {
        case 0,1,2:
        default:
            return errors.New("dnstap: Error: outputs OutputKafka.Compresion is 0(none),1(gzip) or 2(snappy).\n")
        }
    }
    return nil
}

func LoadConfig(configFile string) (*Config) {
    conf := &Config{}
    // default setting
    conf.OutputKafka.Acks             = 1
    conf.OutputKafka.Max_send_retries = 3
    conf.OutputKafka.Flush_interval   = 1000
    _, err := toml.DecodeFile(configFile, &conf)
    if err != nil {
        fmt.Fprintf(os.Stderr, "dnstap. Error: Failed to open or Varidate Error, toml file: \n",)
        os.Exit(1)
    }
    return conf
}

func LoadArg(r_tcp, r_file, r_sock, fname string, doAppend, text, yaml, json bool) (*Config) {
    conf := &Config{}
    if r_tcp != "" {
        conf.Input.Type = "tcp"
        conf.Input.Path = r_tcp
    } else if r_file != "" {
        conf.Input.Type = "file"
        conf.Input.Path = r_file
    } else if r_sock != "" {
        conf.Input.Type = "sock"
        conf.Input.Path = r_sock
    }
    if text {
        conf.OutputFile.Format = "text"
    } else if yaml {
        conf.OutputFile.Format = "yaml"
    } else if json {
        conf.OutputFile.Format = "json"
    } else if fname == "" || fname == "-" {
        conf.OutputFile.Format = "text"
    }
    conf.OutputFile.Path   = fname
    conf.OutputFile.Append = doAppend
    return conf
}

func NewLoadConfig(conf_file, r_tcp, r_file, r_sock, fname string, doAppend, text, yaml, json bool) (*Config, error) {
    conf := &Config{}
    if conf_file != "" {
        conf = LoadConfig(conf_file)
    } else {
        conf = LoadArg(r_tcp, r_file, r_sock, fname, doAppend, text, yaml, json)
    }
    if err := conf.validate(); err != nil {
        log.Fatal(err)
        os.Exit(1)
    }
    return conf, nil
}
