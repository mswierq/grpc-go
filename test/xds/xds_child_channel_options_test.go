/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package xds_test

import (
	"context"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	experimental "google.golang.org/grpc/experimental/opentelemetry"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/internal/envconfig"
	"google.golang.org/grpc/internal/stubserver"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/internal/testutils/rls"
	"google.golang.org/grpc/internal/testutils/xds/e2e"
	"google.golang.org/grpc/internal/testutils/xds/e2e/setup"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/stats/opentelemetry"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	v3corepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	v3listenerpb "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	v3procfilterpb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	v3httppb "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	v3discoverypb "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	v3procservicegrpc "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	v3procservicepb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	rlspb "google.golang.org/grpc/internal/proto/grpc_lookup_v1"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	testpb "google.golang.org/grpc/interop/grpc_testing"

	_ "google.golang.org/grpc/balancer/rls" // Register the RLS Load Balancing policy.
)

// This file contains end-to-end tests for gRFC A110 (child channel options),
// covering all the internal child channels created by gRPC on behalf of a
// parent channel or server: the xDS control plane channel, the RLS control
// channel and the ext_proc external processor channel.

const (
	childOptsADSMethod     = "/envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources"
	childOptsADSSpan       = "Sent.envoy.service.discovery.v3.AggregatedDiscoveryService.StreamAggregatedResources"
	childOptsRLSMethodAttr = "grpc.lookup.v1.RouteLookupService/RouteLookup"
	childOptsRLSSpan       = "Sent.grpc.lookup.v1.RouteLookupService.RouteLookup"
	childOptsExtProcSpan   = "Sent.envoy.service.ext_proc.v3.ExternalProcessor.Process"
	emptyCallMethod        = "/grpc.testing.TestService/EmptyCall"
	emptyCallAttr          = "grpc.testing.TestService/EmptyCall"
	emptyCallSpan          = "Sent.grpc.testing.TestService.EmptyCall"
	fullDuplexCallMethod   = "/grpc.testing.TestService/FullDuplexCall"
	testServiceName        = "grpc.testing.TestService"

	// The xDS client (ADS stream) and the ext_proc filter (Process stream)
	// create their streams without the grpc.StaticMethod call option, so the
	// OpenTelemetry plugin records their method as "other". These RPCs are
	// distinguished by the target of their channel instead.
	otelOtherMethodAttr = "other"

	listenerTypeURL      = "type.googleapis.com/envoy.config.listener.v3.Listener"
	insecureCredsTypeURL = "type.googleapis.com/envoy.extensions.grpc_service.channel_credentials.insecure.v3.InsecureCredentials"

	otelClientAttemptStarted = "grpc.client.attempt.started"
	otelServerCallStarted    = "grpc.server.call.started"
)

// otelCallKey identifies RPCs recorded by OpenTelemetry metrics by their
// grpc.target and grpc.method attributes.
type otelCallKey struct {
	target string
	method string
}

// testOTel is an OpenTelemetry metrics and tracing setup whose
// recorded data can be inspected by tests.
type testOTel struct {
	reader *sdkmetric.ManualReader
	spans  *tracetest.SpanRecorder
	opts   opentelemetry.Options
}

func newTestOTel() *testOTel {
	reader := sdkmetric.NewManualReader()
	spans := tracetest.NewSpanRecorder()
	return &testOTel{
		reader: reader,
		spans:  spans,
		opts: opentelemetry.Options{
			MetricsOptions: opentelemetry.MetricsOptions{
				MeterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)),
				Metrics:       opentelemetry.DefaultMetrics(),
			},
			TraceOptions: experimental.TraceOptions{
				TracerProvider:    sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)),
				TextMapPropagator: propagation.NewCompositeTextMapPropagator(opentelemetry.GRPCTraceBinPropagator{}),
			},
		},
	}
}

// counts returns the values of the named int64 counter metric, keyed by the
// grpc.target and grpc.method attributes of the data points.
func (ct *testOTel) counts(t *testing.T, name string) map[otelCallKey]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := ct.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}
	got := make(map[otelCallKey]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("Metric %q has data of type %T, want metricdata.Sum[int64]", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				target, _ := dp.Attributes.Value("grpc.target")
				method, _ := dp.Attributes.Value("grpc.method")
				got[otelCallKey{target: target.AsString(), method: method.AsString()}] += dp.Value
			}
		}
	}
	return got
}

// spanNames returns the names of all the spans started so far.
func (ct *testOTel) spanNames() []string {
	var names []string
	for _, s := range ct.spans.Started() {
		names = append(names, s.Name())
	}
	return names
}

// spansNamed returns all the started spans with the given name.
func (ct *testOTel) spansNamed(name string) []sdktrace.ReadWriteSpan {
	var spans []sdktrace.ReadWriteSpan
	for _, s := range ct.spans.Started() {
		if s.Name() == name {
			spans = append(spans, s)
		}
	}
	return spans
}

// waitForClientAttempts waits until client attempts were recorded for all the
// given keys, and returns all the recorded client attempts.
func (ct *testOTel) waitForClientAttempts(ctx context.Context, t *testing.T, want ...otelCallKey) map[otelCallKey]int64 {
	t.Helper()
	for {
		got := ct.counts(t, otelClientAttemptStarted)
		if !slices.ContainsFunc(want, func(k otelCallKey) bool { return got[k] == 0 }) {
			return got
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for client attempts %v to be recorded, got: %v", want, got)
		case <-time.After(defaultTestShortTimeout):
		}
	}
}

// waitForSpans waits until spans with all the given names were started.
func (ct *testOTel) waitForSpans(ctx context.Context, t *testing.T, want ...string) {
	t.Helper()
	for {
		got := ct.spanNames()
		if !slices.ContainsFunc(want, func(n string) bool { return !slices.Contains(got, n) }) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for spans %v to be started, got: %v", want, got)
		case <-time.After(defaultTestShortTimeout):
		}
	}
}

// verifyChildTelemetryHasNoParentTraffic verifies that the child telemetry did
// not record any RPC of the parent channel.
func verifyChildTelemetryHasNoParentTraffic(t *testing.T, child *testOTel, parentTarget string) {
	t.Helper()
	for k := range child.counts(t, otelClientAttemptStarted) {
		if k.target == parentTarget || strings.HasPrefix(k.method, testServiceName) {
			t.Errorf("Child telemetry recorded parent channel RPC attempt %+v", k)
		}
	}
	for _, n := range child.spanNames() {
		if strings.Contains(n, testServiceName) {
			t.Errorf("Child telemetry recorded parent channel span %q", n)
		}
	}
}

// verifyParentTelemetry verifies that the parent telemetry recorded exactly
// the wanted client attempts, i.e. the parent channel's own RPCs without any
// child channel RPC, and only spans of the parent channel's own RPCs.
func verifyParentTelemetry(t *testing.T, parent *testOTel, want map[otelCallKey]int64) {
	t.Helper()
	if diff := cmp.Diff(want, parent.counts(t, otelClientAttemptStarted), cmp.AllowUnexported(otelCallKey{})); diff != "" {
		t.Errorf("Parent telemetry recorded unexpected client attempts (-want +got):\n%s", diff)
	}
	for _, n := range parent.spanNames() {
		if !strings.Contains(n, testServiceName) {
			t.Errorf("Parent telemetry recorded child channel span %q", n)
		}
	}
}

// stringSet is a thread-safe set of strings.
type stringSet struct {
	mu   sync.Mutex
	strs map[string]bool
}

func (s *stringSet) add(str string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.strs == nil {
		s.strs = make(map[string]bool)
	}
	s.strs[str] = true
}

func (s *stringSet) has(str string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.strs[str]
}

func (s *stringSet) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(slices.Sorted(maps.Keys(s.strs)), ", ")
}

// ldsRecorder is a stream interceptor that records the Listener
// resource names requested on ADS streams that it intercepts. It allows
// identifying which xDS client created an ADS stream, since every xDS client
// requests the Listener resource named after the target of its channel.
type ldsRecorder struct {
	names stringSet
}

func (r *ldsRecorder) streamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	cs, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil || method != childOptsADSMethod {
		return cs, err
	}
	return &ldsRecordingStream{ClientStream: cs, r: r}, nil
}

func (r *ldsRecorder) waitFor(ctx context.Context, t *testing.T, names ...string) {
	t.Helper()
	for {
		if !slices.ContainsFunc(names, func(n string) bool { return !r.names.has(n) }) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for Listener resources %v to be requested on intercepted ADS streams, got: [%s]", names, r.names.String())
		case <-time.After(defaultTestShortTimeout):
		}
	}
}

type ldsRecordingStream struct {
	grpc.ClientStream
	r *ldsRecorder
}

func (s *ldsRecordingStream) SendMsg(m any) error {
	// The xDS client sends requests already marshaled.
	if b, ok := m.([]byte); ok {
		req := &v3discoverypb.DiscoveryRequest{}
		if err := proto.Unmarshal(b, req); err == nil && req.GetTypeUrl() == listenerTypeURL {
			for _, n := range req.GetResourceNames() {
				s.r.names.add(n)
			}
		}
	}
	return s.ClientStream.SendMsg(m)
}

// continueExtProcServer is an external processor that lets every request
// continue without modifications. It expects to only receive request headers.
type continueExtProcServer struct {
	v3procservicegrpc.UnimplementedExternalProcessorServer
}

func (continueExtProcServer) Process(stream v3procservicegrpc.ExternalProcessor_ProcessServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if req.GetRequestHeaders() == nil {
			return status.Errorf(codes.InvalidArgument, "unexpected message from client: %v", req)
		}
		resp := &v3procservicepb.ProcessingResponse{
			Response: &v3procservicepb.ProcessingResponse_RequestHeaders{
				RequestHeaders: &v3procservicepb.HeadersResponse{
					Response: &v3procservicepb.CommonResponse{Status: v3procservicepb.CommonResponse_CONTINUE},
				},
			},
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// startExtProcServer enables the ext_proc filter on the client for
// the duration of the test, and starts an external processor server. It
// returns the address of the server.
func startExtProcServer(t *testing.T) string {
	t.Helper()
	testutils.SetEnvConfig(t, &envconfig.XDSClientExtProcEnabled, true)
	t.Cleanup(internal.RegisterExtProcFilterForTesting())

	lis, err := testutils.LocalTCPListener()
	if err != nil {
		t.Fatalf("testutils.LocalTCPListener() failed: %v", err)
	}
	srv := grpc.NewServer()
	v3procservicegrpc.RegisterExternalProcessorServer(srv, continueExtProcServer{})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// addExtProcFilter adds an ext_proc filter, which sends request
// headers to the external processor at extProcTarget, to the given client
// Listener resource.
func addExtProcFilter(t *testing.T, lis *v3listenerpb.Listener, extProcTarget string) {
	t.Helper()
	hcm := &v3httppb.HttpConnectionManager{}
	if err := lis.GetApiListener().GetApiListener().UnmarshalTo(hcm); err != nil {
		t.Fatalf("Failed to unmarshal HttpConnectionManager: %v", err)
	}
	extProcCfg := &v3procfilterpb.ExternalProcessor{
		GrpcService: &v3corepb.GrpcService{
			TargetSpecifier: &v3corepb.GrpcService_GoogleGrpc_{
				GoogleGrpc: &v3corepb.GrpcService_GoogleGrpc{
					TargetUri:                extProcTarget,
					ChannelCredentialsPlugin: []*anypb.Any{{TypeUrl: insecureCredsTypeURL}},
				},
			},
		},
		ProcessingMode: &v3procfilterpb.ProcessingMode{
			RequestHeaderMode:  v3procfilterpb.ProcessingMode_SEND,
			ResponseHeaderMode: v3procfilterpb.ProcessingMode_SKIP,
		},
	}
	hcm.HttpFilters = append([]*v3httppb.HttpFilter{e2e.HTTPFilter("extproc", extProcCfg)}, hcm.HttpFilters...)
	hcmAny := testutils.MarshalAny(t, hcm)
	lis.ApiListener.ApiListener = hcmAny
	lis.FilterChains[0].Filters[0].ConfigType = &v3listenerpb.Filter_TypedConfig{TypedConfig: hcmAny}
}

// TestChildChannelOptions_OpenTelemetry_XDSControlPlane verifies that
// OpenTelemetry instrumentation passed in the child channel options of an xDS
// client channel is applied to the xDS control plane channel, which emits
// metrics and traces labeled with its own target and method, while the
// parent channel's own instrumentation records only the parent channel's RPCs
// without any child channel traffic.
func (s) TestChildChannelOptions_OpenTelemetry_XDSControlPlane(t *testing.T) {
	managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)
	server := stubserver.StartTestService(t, nil)
	defer server.Stop()

	const serviceName = "my-service-child-options-otel"
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := managementServer.Update(ctx, e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       "localhost",
		Port:       testutils.ParsePort(t, server.Address),
		SecLevel:   e2e.SecurityLevelNone,
	})); err != nil {
		t.Fatal(err)
	}

	parentTel, childTel := newTestOTel(), newTestOTel()
	cc, err := grpc.NewClient("xds:///"+serviceName,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(xdsResolver),
		opentelemetry.DialOption(parentTel.opts),
		grpc.WithChildChannelOptions(opentelemetry.DialOption(childTel.opts)),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() failed: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}

	parentTarget := "xds:///" + serviceName
	childTel.waitForClientAttempts(ctx, t, otelCallKey{target: "passthrough:///" + managementServer.Address, method: otelOtherMethodAttr})
	childTel.waitForSpans(ctx, t, childOptsADSSpan)
	verifyChildTelemetryHasNoParentTraffic(t, childTel, parentTarget)
	verifyParentTelemetry(t, parentTel, map[otelCallKey]int64{{target: parentTarget, method: emptyCallAttr}: 1})
}

// TestChildChannelOptions_OpenTelemetry_RLS verifies that OpenTelemetry
// instrumentation passed in the child channel options of an xDS client
// channel is applied to the RLS control channel created by the RLS LB policy
// (configured via the RLS cluster specifier plugin), as well as to the xDS
// control plane channel, and that the parent channel's metrics do not count
// any of their traffic.
func (s) TestChildChannelOptions_OpenTelemetry_RLS(t *testing.T) {
	managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)
	server := stubserver.StartTestService(t, nil)
	defer server.Stop()

	rlsServer, _ := rls.SetupFakeRLSServer(t, nil)
	const serviceName = "my-service-child-options-rls"
	rlsServer.SetResponseCallback(func(context.Context, *rlspb.RouteLookupRequest) *rls.RouteLookupResponse {
		return &rls.RouteLookupResponse{Resp: &rlspb.RouteLookupResponse{Targets: []string{"cluster-" + serviceName}}}
	})
	rlsProto := &rlspb.RouteLookupConfig{
		GrpcKeybuilders:      []*rlspb.GrpcKeyBuilder{{Names: []*rlspb.GrpcKeyBuilder_Name{{Service: testServiceName}}}},
		LookupService:        rlsServer.Address,
		LookupServiceTimeout: durationpb.New(defaultTestTimeout),
		CacheSizeBytes:       1024,
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := managementServer.Update(ctx, defaultClientResourcesWithRLSCSP(t, e2e.LoadBalancingPolicyRoundRobin, e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       "localhost",
		Port:       testutils.ParsePort(t, server.Address),
		SecLevel:   e2e.SecurityLevelNone,
	}, rlsProto)); err != nil {
		t.Fatal(err)
	}

	parentTel, childTel := newTestOTel(), newTestOTel()
	cc, err := grpc.NewClient("xds:///"+serviceName,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(xdsResolver),
		opentelemetry.DialOption(parentTel.opts),
		grpc.WithChildChannelOptions(opentelemetry.DialOption(childTel.opts)),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() failed: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)
	// The route has no default target, so the RPC can only be routed once the
	// RLS lookup on the RLS control channel completes.
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}

	parentTarget := "xds:///" + serviceName
	childTel.waitForClientAttempts(ctx, t,
		otelCallKey{target: "passthrough:///" + managementServer.Address, method: otelOtherMethodAttr},
		otelCallKey{target: "dns:///" + rlsServer.Address, method: childOptsRLSMethodAttr},
	)
	childTel.waitForSpans(ctx, t, childOptsADSSpan, childOptsRLSSpan)
	verifyChildTelemetryHasNoParentTraffic(t, childTel, parentTarget)
	verifyParentTelemetry(t, parentTel, map[otelCallKey]int64{{target: parentTarget, method: emptyCallAttr}: 1})
}

// TestChildChannelOptions_OpenTelemetry_ExtProc verifies that OpenTelemetry
// instrumentation passed in the child channel options of an xDS client
// channel is applied to the channel created by the ext_proc filter to the
// external processor server. The Process stream must be recorded by the
// child metrics with the external processor channel's own target and method
// (even though it is started in the context of a parent channel RPC), and
// its span must be part of the trace of the parent channel RPC. The parent
// channel's metrics must not count the Process stream.
func (s) TestChildChannelOptions_OpenTelemetry_ExtProc(t *testing.T) {
	extProcAddr := startExtProcServer(t)
	managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)
	server := stubserver.StartTestService(t, nil)
	defer server.Stop()

	const serviceName = "my-service-child-options-ext-proc"
	resources := e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       "localhost",
		Port:       testutils.ParsePort(t, server.Address),
		SecLevel:   e2e.SecurityLevelNone,
	})
	addExtProcFilter(t, resources.Listeners[0], extProcAddr)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := managementServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	parentTel, childTel := newTestOTel(), newTestOTel()
	cc, err := grpc.NewClient("xds:///"+serviceName,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(xdsResolver),
		opentelemetry.DialOption(parentTel.opts),
		grpc.WithChildChannelOptions(opentelemetry.DialOption(childTel.opts)),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() failed: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}

	parentTarget := "xds:///" + serviceName
	childTel.waitForClientAttempts(ctx, t,
		otelCallKey{target: "passthrough:///" + managementServer.Address, method: otelOtherMethodAttr},
		otelCallKey{target: "dns:///" + extProcAddr, method: otelOtherMethodAttr},
	)
	childTel.waitForSpans(ctx, t, childOptsADSSpan, childOptsExtProcSpan)
	verifyChildTelemetryHasNoParentTraffic(t, childTel, parentTarget)
	verifyParentTelemetry(t, parentTel, map[otelCallKey]int64{{target: parentTarget, method: emptyCallAttr}: 1})

	// The Process stream is started in the context of the parent channel RPC,
	// so the trace context must be propagated from the parent channel's call
	// span to the external processor channel's call span.
	parentSpans := parentTel.spansNamed(emptyCallSpan)
	if len(parentSpans) != 1 {
		t.Fatalf("Parent telemetry recorded %d %q spans, want 1", len(parentSpans), emptyCallSpan)
	}
	procSpans := childTel.spansNamed(childOptsExtProcSpan)
	if len(procSpans) != 1 {
		t.Fatalf("Child telemetry recorded %d %q spans, want 1", len(procSpans), childOptsExtProcSpan)
	}
	parentSC, procSC := parentSpans[0].SpanContext(), procSpans[0].SpanContext()
	if got, want := procSC.TraceID(), parentSC.TraceID(); got != want {
		t.Errorf("Span %q has trace ID %v, want trace ID %v of parent channel span %q", childOptsExtProcSpan, got, want, emptyCallSpan)
	}
	if got, want := procSpans[0].Parent().SpanID(), parentSC.SpanID(); got != want {
		t.Errorf("Span %q has parent span ID %v, want span ID %v of parent channel span %q", childOptsExtProcSpan, got, want, emptyCallSpan)
	}
}

// TestChildChannelOptions_MultiLevelNesting verifies the recursive
// propagation of child channel options across multiple levels of nesting:
//
//	Parent channel P (xds:///<service>)
//	  -> Child channel C: ext_proc channel to xds:///<ext-proc-service>
//	    -> Grandchild channel G: xDS control plane channel of C's xDS client
//
// The child channel options O_child configured on P must be applied to C, and
// automatically inherited by G, without being applied to P itself.
func (s) TestChildChannelOptions_MultiLevelNesting(t *testing.T) {
	extProcAddr := startExtProcServer(t)
	managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)
	server := stubserver.StartTestService(t, nil)
	defer server.Stop()

	// Resources for P, with an ext_proc filter whose external processor is
	// itself reached over xDS, and resources for C, pointing to the external
	// processor server.
	const (
		serviceName        = "my-service-child-options-nesting"
		extProcServiceName = "ext-proc-service-child-options-nesting"
	)
	resources := e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       "localhost",
		Port:       testutils.ParsePort(t, server.Address),
		SecLevel:   e2e.SecurityLevelNone,
	})
	addExtProcFilter(t, resources.Listeners[0], "xds:///"+extProcServiceName)
	extProcResources := e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: extProcServiceName,
		NodeID:     nodeID,
		Host:       "localhost",
		Port:       testutils.ParsePort(t, extProcAddr),
		SecLevel:   e2e.SecurityLevelNone,
	})
	resources.Listeners = append(resources.Listeners, extProcResources.Listeners...)
	resources.Routes = append(resources.Routes, extProcResources.Routes...)
	resources.Clusters = append(resources.Clusters, extProcResources.Clusters...)
	resources.Endpoints = append(resources.Endpoints, extProcResources.Endpoints...)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := managementServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	// O_child carries OpenTelemetry instrumentation, an interceptor
	// identifying the xDS clients creating ADS streams, and the xDS resolver
	// that C needs to resolve the target of the external processor. Every
	// xDS channel built by this resolver gets its own xDS client.
	parentTel, childTel := newTestOTel(), newTestOTel()
	// recorder verifies that G inherited O_child. The ADS streams of P's
	// control plane channel and of G target the same management server and
	// use the same method, so their telemetry is indistinguishable, and the
	// telemetry checks below would pass even if O_child was applied only to
	// P's control plane channel. Since every xDS client requests the Listener
	// named after the target of its channel, observing the Listener for C's
	// target being requested on an ADS stream intercepted by O_child proves
	// that the stream belongs to G.
	recorder := &ldsRecorder{}
	cc, err := grpc.NewClient("xds:///"+serviceName,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(xdsResolver),
		opentelemetry.DialOption(parentTel.opts),
		grpc.WithChildChannelOptions(
			opentelemetry.DialOption(childTel.opts),
			grpc.WithChainStreamInterceptor(recorder.streamInterceptor),
			grpc.WithResolvers(xdsResolver),
		),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() failed: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}

	// The ADS stream of P's xDS client requests the Listener for P's target,
	// while the ADS stream of C's xDS client (on G) requests the Listener for
	// C's target. Both must have been intercepted by O_child.
	recorder.waitFor(ctx, t, serviceName, extProcServiceName)

	// C must be instrumented by O_child, and record the Process stream with
	// its own target.
	parentTarget := "xds:///" + serviceName
	childTel.waitForClientAttempts(ctx, t,
		otelCallKey{target: "passthrough:///" + managementServer.Address, method: otelOtherMethodAttr},
		otelCallKey{target: "xds:///" + extProcServiceName, method: otelOtherMethodAttr},
	)
	childTel.waitForSpans(ctx, t, childOptsADSSpan, childOptsExtProcSpan)
	verifyChildTelemetryHasNoParentTraffic(t, childTel, parentTarget)
	verifyParentTelemetry(t, parentTel, map[otelCallKey]int64{{target: parentTarget, method: emptyCallAttr}: 1})
}

// TestChildChannelOptions_OpenTelemetry_XDSServer verifies that OpenTelemetry
// instrumentation passed in the child channel options of an xDS-enabled gRPC
// server is applied to the server's xDS control plane channel, while the
// server's own instrumentation records only the server's RPCs.
func (s) TestChildChannelOptions_OpenTelemetry_XDSServer(t *testing.T) {
	managementServer, nodeID, bootstrapContents, xdsResolver := setup.ManagementServerAndResolver(t)

	serverTel, childTel := newTestOTel(), newTestOTel()
	lis, cleanup := setupGRPCServer(t, bootstrapContents,
		opentelemetry.ServerOption(serverTel.opts),
		grpc.ChildChannelOptions(opentelemetry.DialOption(childTel.opts)),
	)
	defer cleanup()

	host, port, err := hostPortFromListener(lis)
	if err != nil {
		t.Fatalf("Failed to retrieve host and port of server: %v", err)
	}
	const serviceName = "my-service-child-options-server"
	resources := e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       host,
		Port:       port,
		SecLevel:   e2e.SecurityLevelNone,
	})
	resources.Listeners = append(resources.Listeners, e2e.DefaultServerListener(host, port, e2e.SecurityLevelNone, "routeName"))
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := managementServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	cc, err := grpc.NewClient("xds:///"+serviceName, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("grpc.NewClient() failed: %v", err)
	}
	defer cc.Close()
	client := testgrpc.NewTestServiceClient(cc)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("EmptyCall() failed: %v", err)
	}

	// The server's xDS control plane channel is instrumented by the child
	// channel options, and only records client-side RPCs.
	childTel.waitForClientAttempts(ctx, t, otelCallKey{target: "passthrough:///" + managementServer.Address, method: otelOtherMethodAttr})
	childTel.waitForSpans(ctx, t, childOptsADSSpan)
	if got := childTel.counts(t, otelServerCallStarted); len(got) != 0 {
		t.Errorf("Child telemetry recorded server calls: %v", got)
	}

	// The server's own instrumentation records only the server's RPCs, and
	// none of the xDS control plane channel's RPCs.
	wantServerCalls := map[otelCallKey]int64{{method: emptyCallAttr}: 1}
	if diff := cmp.Diff(wantServerCalls, serverTel.counts(t, otelServerCallStarted), cmp.AllowUnexported(otelCallKey{})); diff != "" {
		t.Errorf("Server telemetry recorded unexpected server calls (-want +got):\n%s", diff)
	}
	if got := serverTel.counts(t, otelClientAttemptStarted); len(got) != 0 {
		t.Errorf("Server telemetry recorded client attempts: %v", got)
	}
	for _, n := range serverTel.spanNames() {
		if !strings.Contains(n, testServiceName) {
			t.Errorf("Server telemetry recorded child channel span %q", n)
		}
	}
}

// TestChildChannelOptions_EmptyOrUnset verifies that, when child channel
// options are not set, are empty, or are reset by a later empty
// grpc.WithChildChannelOptions, xDS client channels work as usual, and the
// parent channel's own dial options are not implicitly applied to the child
// channels.
func (s) TestChildChannelOptions_EmptyOrUnset(t *testing.T) {
	// discardedTel is passed to child channel options which are reset by a
	// later grpc.WithChildChannelOptions, and so must never record anything.
	discardedTel := newTestOTel()
	tests := []struct {
		name     string
		dialOpts []grpc.DialOption
	}{
		{
			name: "unset",
		},
		{
			name:     "empty",
			dialOpts: []grpc.DialOption{grpc.WithChildChannelOptions()},
		},
		{
			name: "reset by later empty option",
			dialOpts: []grpc.DialOption{
				grpc.WithChildChannelOptions(opentelemetry.DialOption(discardedTel.opts)),
				grpc.WithChildChannelOptions(),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)
			server := stubserver.StartTestService(t, nil)
			defer server.Stop()

			const serviceName = "my-service-child-options-empty"
			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			defer cancel()
			if err := managementServer.Update(ctx, e2e.DefaultClientResources(e2e.ResourceParams{
				DialTarget: serviceName,
				NodeID:     nodeID,
				Host:       "localhost",
				Port:       testutils.ParsePort(t, server.Address),
				SecLevel:   e2e.SecurityLevelNone,
			})); err != nil {
				t.Fatal(err)
			}

			parentTel := newTestOTel()
			dialOpts := append([]grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithResolvers(xdsResolver),
				opentelemetry.DialOption(parentTel.opts),
			}, test.dialOpts...)
			cc, err := grpc.NewClient("xds:///"+serviceName, dialOpts...)
			if err != nil {
				t.Fatalf("grpc.NewClient() failed: %v", err)
			}
			defer cc.Close()

			// A successful RPC implies that the xDS control plane channel was
			// created and used to fetch the resources.
			client := testgrpc.NewTestServiceClient(cc)
			if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); err != nil {
				t.Fatalf("EmptyCall() failed: %v", err)
			}

			verifyParentTelemetry(t, parentTel, map[otelCallKey]int64{{target: "xds:///" + serviceName, method: emptyCallAttr}: 1})
			if got := discardedTel.counts(t, otelClientAttemptStarted); len(got) != 0 {
				t.Errorf("Telemetry from reset child channel options recorded client attempts: %v", got)
			}
			if got := discardedTel.spanNames(); len(got) != 0 {
				t.Errorf("Telemetry from reset child channel options recorded spans: %v", got)
			}
		})
	}
}

// TestChildChannelOptions_GlobalDialOptions verifies the interaction between
// global dial options and child channel options on child channels. Global
// dial options apply to all channels, but child channel options are applied
// after them, so they override non-cumulative global options (such as a
// stream interceptor), are combined with cumulative ones (such as stats
// handlers), and can disable global dial options for child channels
// altogether. The parent channel is not affected by child channel options.
func (s) TestChildChannelOptions_GlobalDialOptions(t *testing.T) {
	tests := []struct {
		name string
		// disableGlobalOptsInChild specifies whether the child channel options
		// disable the global dial options.
		disableGlobalOptsInChild bool
	}{
		{
			name: "child options combined with global options",
		},
		{
			name:                     "child options disable global options",
			disableGlobalOptsInChild: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			managementServer, nodeID, _, xdsResolver := setup.ManagementServerAndResolver(t)
			server := stubserver.StartTestService(t, &stubserver.StubServer{
				EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
					return &testpb.Empty{}, nil
				},
				FullDuplexCallF: func(stream testgrpc.TestService_FullDuplexCallServer) error {
					for {
						if _, err := stream.Recv(); err != nil {
							if err == io.EOF {
								return nil
							}
							return err
						}
					}
				},
			})
			defer server.Stop()

			const serviceName = "my-service-child-options-global"
			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			defer cancel()
			if err := managementServer.Update(ctx, e2e.DefaultClientResources(e2e.ResourceParams{
				DialTarget: serviceName,
				NodeID:     nodeID,
				Host:       "localhost",
				Port:       testutils.ParsePort(t, server.Address),
				SecLevel:   e2e.SecurityLevelNone,
			})); err != nil {
				t.Fatal(err)
			}

			var globalStreamMethods, globalStatsMethods, childStreamMethods stringSet
			globalStreamInt := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				globalStreamMethods.add(method)
				return streamer(ctx, desc, cc, method, opts...)
			}
			globalStatsHandler := &testutils.StubStatsHandler{
				TagRPCF: func(ctx context.Context, info *stats.RPCTagInfo) context.Context {
					globalStatsMethods.add(info.FullMethodName)
					return ctx
				},
			}
			childStreamInt := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				childStreamMethods.add(method)
				return streamer(ctx, desc, cc, method, opts...)
			}

			internal.AddGlobalDialOptions.(func(opt ...grpc.DialOption))(grpc.WithStreamInterceptor(globalStreamInt), grpc.WithStatsHandler(globalStatsHandler))
			t.Cleanup(internal.ClearGlobalDialOptions)

			childOpts := []grpc.DialOption{grpc.WithStreamInterceptor(childStreamInt)}
			if test.disableGlobalOptsInChild {
				childOpts = append(childOpts, internal.DisableGlobalDialOptions.(func() grpc.DialOption)())
			}
			cc, err := grpc.NewClient("xds:///"+serviceName,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithResolvers(xdsResolver),
				grpc.WithChildChannelOptions(childOpts...),
			)
			if err != nil {
				t.Fatalf("grpc.NewClient() failed: %v", err)
			}
			defer cc.Close()

			client := testgrpc.NewTestServiceClient(cc)
			if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); err != nil {
				t.Fatalf("EmptyCall() failed: %v", err)
			}
			stream, err := client.FullDuplexCall(ctx)
			if err != nil {
				t.Fatalf("FullDuplexCall() failed: %v", err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatalf("stream.CloseSend() failed: %v", err)
			}
			if _, err := stream.Recv(); err != io.EOF {
				t.Fatalf("stream.Recv() returned error %v, want %v", err, io.EOF)
			}

			// The ADS stream is created on the xDS control plane channel before
			// the resources needed by the RPCs above are received, so all the
			// interceptors and stats handlers have been invoked by now.

			// The child stream interceptor overrides the global one on the
			// child channel, and is not applied to the parent channel.
			if !childStreamMethods.has(childOptsADSMethod) {
				t.Errorf("Child stream interceptor did not intercept method %q, intercepted: [%s]", childOptsADSMethod, childStreamMethods.String())
			}
			if childStreamMethods.has(fullDuplexCallMethod) {
				t.Errorf("Child stream interceptor unexpectedly intercepted parent channel method %q", fullDuplexCallMethod)
			}
			if globalStreamMethods.has(childOptsADSMethod) {
				t.Errorf("Global stream interceptor unexpectedly intercepted child channel method %q", childOptsADSMethod)
			}
			if !globalStreamMethods.has(fullDuplexCallMethod) {
				t.Errorf("Global stream interceptor did not intercept parent channel method %q, intercepted: [%s]", fullDuplexCallMethod, globalStreamMethods.String())
			}

			// The global stats handler always applies to the parent channel,
			// and applies to the child channel only if not disabled by the
			// child channel options.
			for _, m := range []string{emptyCallMethod, fullDuplexCallMethod} {
				if !globalStatsMethods.has(m) {
					t.Errorf("Global stats handler did not observe parent channel method %q, observed: [%s]", m, globalStatsMethods.String())
				}
			}
			if got, want := globalStatsMethods.has(childOptsADSMethod), !test.disableGlobalOptsInChild; got != want {
				t.Errorf("Global stats handler observed child channel method %q: %v, want %v", childOptsADSMethod, got, want)
			}
		})
	}
}
