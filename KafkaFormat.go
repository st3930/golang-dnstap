/*
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
        "bytes"
        "encoding/json"
        "fmt"
        "net"
        "time"

        "github.com/miekg/dns"
)

type KafkaTime time.Time

const TimeFormat = "2006-01-02 15:04:05"

func (jt *KafkaTime) MarshalJSON() ([]byte, error) {
        stamp := time.Time(*jt).Format(TimeFormat)
        return []byte(fmt.Sprintf("\"%s\"", stamp)), nil
}

type KafkaMessage struct {
        Type                 string    `json:"type"`
        Identity             string    `json:"identity,omitempty"`
        Version              string    `json:"version,omitempty"`
        TimeStamp            *KafkaTime `json:"timestamp,omitempty"`
        MessageType          string    `json:"msg_type"`
        SocketFamily         string    `json:"socket_family,omitempty"`
        SocketProtocol       string    `json:"socket_protocol,omitempty"`
        QueryAddress         *net.IP   `json:"query_address,omitempty"`
        ResponseAddress      *net.IP   `json:"response_address,omitempty"`
        QueryPort            uint32    `json:"query_port,omitempty"`
        ResponsePort         uint32    `json:"response_port,omitempty"`
        QueryZone            string    `json:"query_zone,omitempty"`
        QueryName            string    `json:"query_name,omitempty"`
        QueryType            string    `json:"query_type,omitempty"`
        QueryID              uint16    `json:"query_id,omitempty"`
        Rcode                string    `json:"rcode,omitempty"`
        FlagsAuth            int       `json:"flags_aa,omitempty"`
        FlagsTruncate        int       `json:"flags_tc,omitempty"`
        FlagsRecDes          int       `json:"flags_rd,omitempty"`
        FlagsRecAva          int       `json:"flags_ra,omitempty"`
        FlagsAuthenticate    int       `json:"flags_ad,omitempty"`
        FlagsChkDis          int       `json:"flags_cd,omitempty"`
}

func b2i(b bool) int{
	if b == true{
		return 1
	}else{
		return 0
	}
}

func convertKafkaMessage(m *Message) (kMsg *KafkaMessage) {
        kMsg = new(KafkaMessage)
        kMsg.MessageType    = fmt.Sprint(m.Type)
        kMsg.SocketFamily   = fmt.Sprint(m.SocketFamily)
        kMsg.SocketProtocol = fmt.Sprint(m.SocketProtocol)

        if m.ResponseTimeSec != nil && m.ResponseTimeNsec != nil {
                rt := KafkaTime(time.Unix(int64(*m.ResponseTimeSec), int64(*m.ResponseTimeNsec)).UTC())
                kMsg.TimeStamp = &rt
        } else if m.QueryTimeSec != nil && m.QueryTimeNsec != nil {
                qt := KafkaTime(time.Unix(int64(*m.QueryTimeSec), int64(*m.QueryTimeNsec)).UTC())
                kMsg.TimeStamp = &qt
        }

        if m.QueryAddress != nil {
                qa := net.IP(m.QueryAddress)
                kMsg.QueryAddress = &qa
        }

        if m.ResponseAddress != nil {
                ra := net.IP(m.ResponseAddress)
                kMsg.ResponseAddress = &ra
        }

        if m.QueryPort != nil {
                kMsg.QueryPort = *m.QueryPort
        }

        if m.ResponsePort != nil {
                kMsg.ResponsePort = *m.ResponsePort
        }

        if m.QueryZone != nil {
                name, _, err := dns.UnpackDomainName(m.QueryZone, 0)
                if err != nil {
                        kMsg.QueryZone = "parse failed"
                } else {
                        kMsg.QueryZone = string(name)
                }
        }

        if m.QueryMessage != nil {
                msg := new(dns.Msg)
                err := msg.Unpack(m.QueryMessage)
                if err != nil || len(msg.Question) == 0 {
                        kMsg.QueryName    = "parse failed"
                } else {
                        kMsg.QueryName    = msg.Question[0].Name
                        kMsg.QueryType    = dns.Type(msg.Question[0].Qtype).String()
                        kMsg.QueryID      = msg.Id
                }
        }

        if m.ResponseMessage != nil {
                msg := new(dns.Msg)
                err := msg.Unpack(m.ResponseMessage)
                if err != nil || len(msg.Question) == 0 {
                        kMsg.QueryName  = "parse failed"
                } else {
                        kMsg.QueryName         = msg.Question[0].Name
                        kMsg.QueryType         = dns.Type(msg.Question[0].Qtype).String()
                        kMsg.QueryID           = msg.Id
                        kMsg.Rcode             = dns.RcodeToString[msg.Rcode]
                        kMsg.FlagsAuth         = b2i(msg.Authoritative)
                        kMsg.FlagsTruncate     = b2i(msg.Truncated)
                        kMsg.FlagsRecDes       = b2i(msg.RecursionDesired)
                        kMsg.FlagsRecAva       = b2i(msg.RecursionAvailable)
                        kMsg.FlagsAuthenticate = b2i(msg.AuthenticatedData)
                        kMsg.FlagsChkDis       = b2i(msg.CheckingDisabled)
                }
        }
        return kMsg
}

func KafkaFormat(dt *Dnstap) (out []byte, ok bool) {
        var s bytes.Buffer

        kafkaMessage         := convertKafkaMessage(dt.Message)
        kafkaMessage.Type     = fmt.Sprint(dt.Type)
        kafkaMessage.Identity = string(dt.Identity)
        kafkaMessage.Version  = string(dt.Version)

        j, _ := json.Marshal(kafkaMessage)

       	s.WriteString(string(j))

        return s.Bytes(), true
}
