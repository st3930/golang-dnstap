/*
 * Copyright (c) 2014 by Farsight Security, Inc.
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
    "log"
    "os"

    "github.com/Shopify/sarama"
    "github.com/golang/protobuf/proto"
)

type KafkaFormatFunc func(*Dnstap) ([]byte, bool)

type KafkaOutput struct {
    format          KafkaFormatFunc
    outputChannel   chan []byte
    wait            chan bool
    kafkaConfig     *sarama.Config
    producer        sarama.AsyncProducer
    topic           string
    key             string
}

func NewKafkaOutput(c KafkaConfig, format KafkaFormatFunc) (o *KafkaOutput, err error) {
    kafkaconfig := sarama.NewConfig()
    kafkaconfig.Net.SASL.Enable          = c.Sasl
    kafkaconfig.Net.SASL.User            = c.User
    kafkaconfig.Net.SASL.Password        = c.Password
    kafkaconfig.Producer.RequiredAcks    = sarama.RequiredAcks(c.Acks)
    kafkaconfig.Producer.Compression     = sarama.CompressionCodec(c.Compression)
    kafkaconfig.Producer.Flush.Frequency = c.Flush_interval
    kafkaconfig.Producer.Retry.Max       = c.Max_send_retries

    o = new(KafkaOutput)
    o.format        = format
    o.outputChannel = make(chan []byte, outputChannelSize)
    o.wait          = make(chan bool)
    o.kafkaConfig   = kafkaconfig
    o.producer, err = sarama.NewAsyncProducer(c.Brokers, o.kafkaConfig)
    o.topic         = c.Topic
    o.key 			= c.Key

    if c.Debug {
        sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
    }

    return
}

func (o *KafkaOutput) GetOutputChannel() chan []byte {
    return o.outputChannel
}

func (o *KafkaOutput) RunOutputLoop() {
    dt := &Dnstap{}
    for frame := range o.outputChannel {
        if err := proto.Unmarshal(frame, dt); err != nil {
            log.Fatalf("dnstap.KafkaOutput: proto.Unmarshal() failed: %s\n", err)
            break
        }
        buf, ok := o.format(dt)
        if !ok {
            log.Fatalf("dnstap.KafkaOutput: text format function failed\n")
            break
        }
        o.producer.Input() <- &sarama.ProducerMessage{
            Topic:	o.topic,
            Key:	sarama.StringEncoder(o.key),
            Value:	sarama.StringEncoder(string(buf)),
        }
    }
    close(o.wait)
}

func (o *KafkaOutput) Close() {
    close(o.outputChannel)
    <-o.wait
    o.producer.Close()
}
