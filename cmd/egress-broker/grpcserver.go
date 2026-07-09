// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	brokerproxypb "github.com/ronlv10/substrate-ws-poc/proto/brokerproxy/v1"
)

// goldenAtespace is where substrate instantiates the golden-snapshot template
// actor. Its proxy announces like any other, but it must never get a Slack
// session: the golden actor is a throwaway whose identity every derived actor
// briefly wears, and a Slack connection keyed to it would sit on the app token.
const goldenAtespace = "ate-golden"

// GRPCServer is the broker's proxy-facing endpoint: one Session stream per
// local proxy. Identity of record is the stream's first frame (Announce) — no
// source-IP correlation, no tickets, no TLS MITM.
type GRPCServer struct {
	brokerproxypb.UnimplementedBrokerProxyServer
	reg     *Registry
	forward *realSlackDialer
	log     *slog.Logger
}

func NewGRPCServer(reg *Registry, forward *realSlackDialer, log *slog.Logger) *GRPCServer {
	return &GRPCServer{reg: reg, forward: forward, log: log}
}

// grpcSink adapts a Session stream to the session's eventSink. sendMu
// serializes all Send calls (events and egress responses come from different
// goroutines; gRPC allows one concurrent sender per stream).
type grpcSink struct {
	stream brokerproxypb.BrokerProxy_SessionServer
	sendMu sync.Mutex
}

func (g *grpcSink) SendEvent(seq uint64, raw []byte) error {
	return g.send(&brokerproxypb.BrokerMsg{Msg: &brokerproxypb.BrokerMsg_Event{
		Event: &brokerproxypb.Event{Seq: seq, Payload: raw},
	}})
}

func (g *grpcSink) send(msg *brokerproxypb.BrokerMsg) error {
	g.sendMu.Lock()
	defer g.sendMu.Unlock()
	return g.stream.Send(msg)
}

// Session handles one proxy connection for its whole lifetime. The stream
// ending (the actor was suspended, or crashed) detaches the sink; buffered
// events wait for the next Announce.
func (s *GRPCServer) Session(stream brokerproxypb.BrokerProxy_SessionServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	announce := first.GetAnnounce()
	if announce == nil {
		return status.Error(codes.InvalidArgument, "first frame must be an Announce")
	}
	if announce.Atespace == "" || announce.Name == "" {
		return status.Error(codes.InvalidArgument, "Announce missing actor identity")
	}

	// The golden template actor: accept and idle the stream (so the template
	// boots and checkpoints cleanly) but never start a Slack session under its
	// identity.
	if announce.Atespace == goldenAtespace {
		s.log.Info("egress-broker: golden template proxy announced; holding stream without a Slack session",
			slog.String("actor", announce.Atespace+"/"+announce.Name))
		for {
			if _, err := stream.Recv(); err != nil {
				return nil
			}
		}
	}

	ref := ActorRef{Atespace: announce.Atespace, Name: announce.Name}
	s.log.Info("egress-broker: proxy announced",
		slog.String("actor", ref.String()), slog.Uint64("last_acked_seq", announce.LastAckedSeq))

	sess := s.reg.GetOrCreate(ref)
	sess.EnsureStarted(announce.AppToken)
	sink := &grpcSink{stream: stream}
	sess.Attach(sink, announce.LastAckedSeq)
	defer func() {
		sess.Detach(sink)
		s.log.Info("egress-broker: proxy stream ended (actor suspended or gone)",
			slog.String("actor", ref.String()))
	}()

	for {
		msg, err := stream.Recv()
		if err != nil {
			return nil
		}
		switch m := msg.Msg.(type) {
		case *brokerproxypb.ProxyMsg_Ack:
			s.log.Info("egress-broker: proxy acked event",
				slog.String("actor", ref.String()), slog.Uint64("seq", m.Ack.Seq))
			sess.Ack(m.Ack.Seq)
		case *brokerproxypb.ProxyMsg_Egress:
			go s.relayEgress(sess, sink, m.Egress)
		case *brokerproxypb.ProxyMsg_Announce:
			return status.Error(codes.InvalidArgument, "duplicate Announce on an open stream")
		}
	}
}

// relayEgress forwards one agent Web API call to real Slack and returns the
// response over the stream. beginForward/endForward hold off idle suspend
// while the call is in flight.
func (s *GRPCServer) relayEgress(sess *session, sink *grpcSink, e *brokerproxypb.Egress) {
	sess.beginForward()
	defer sess.endForward()

	resp := s.doEgress(e)
	if err := sink.send(&brokerproxypb.BrokerMsg{Msg: &brokerproxypb.BrokerMsg_EgressResp{EgressResp: resp}}); err != nil {
		s.log.Warn("egress-broker: sending egress response failed", slog.Any("err", err))
	}
}

func (s *GRPCServer) doEgress(e *brokerproxypb.Egress) *brokerproxypb.EgressResp {
	fail := func(code int, msg string) *brokerproxypb.EgressResp {
		return &brokerproxypb.EgressResp{CorrId: e.CorrId, Status: int32(code), Body: []byte(msg)}
	}
	target := s.forward.apiBaseURL + e.Path
	req, err := http.NewRequest(e.Method, target, strings.NewReader(string(e.Body)))
	if err != nil {
		return fail(http.StatusBadGateway, fmt.Sprintf("building request: %v", err))
	}
	for _, h := range e.Headers {
		for _, v := range h.Values {
			req.Header.Add(h.Name, v)
		}
	}
	req.Header.Del("Accept-Encoding") // let Go negotiate; avoids double-encoding
	req.Host = "slack.com"

	resp, err := s.forward.httpClient.Do(req)
	if err != nil {
		return fail(http.StatusBadGateway, fmt.Sprintf("forwarding to Slack: %v", err))
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fail(http.StatusBadGateway, fmt.Sprintf("reading Slack response: %v", err))
	}

	out := &brokerproxypb.EgressResp{CorrId: e.CorrId, Status: int32(resp.StatusCode), Body: body}
	for k, vv := range resp.Header {
		out.Headers = append(out.Headers, &brokerproxypb.Header{Name: k, Values: vv})
	}
	return out
}
