/*
 *
 * Copyright 2025 gRPC authors.
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

package pool_ext_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcinternal "google.golang.org/grpc/internal"
	"google.golang.org/grpc/internal/grpctest"
	"google.golang.org/grpc/internal/stubserver"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/internal/testutils/xds/e2e"
	"google.golang.org/grpc/internal/xds/bootstrap"
	"google.golang.org/grpc/internal/xds/xdsclient"
	_ "google.golang.org/grpc/xds"

	v3clusterpb "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	v3endpointpb "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	v3listenerpb "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	v3routepb "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	estats "google.golang.org/grpc/experimental/stats"
	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

const defaultTestTimeout = 10 * time.Second

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

// TestDefaultPool_LazyLoadBootstrapConfig verifies that the DefaultPool
// lazily loads the bootstrap configuration from environment variables when
// an xDS client is created for the first time.
//
// If tries to create the new client in DefaultPool at the start of test when
// none of the env vars are set. This should fail.
//
// Then it sets the env var XDSBootstrapFileName and retry creating a client
// in DefaultPool. This should succeed.
func (s) TestDefaultPool_LazyLoadBootstrapConfig(t *testing.T) {
	_, closeFunc, err := xdsclient.DefaultPool.NewClient(t.Name(), &estats.UnimplementedMetricsRecorder{}, nil)
	if err == nil {
		t.Fatalf("xdsclient.DefaultPool.NewClient() succeeded without setting bootstrap config env vars, want failure")
	}

	bs, err := bootstrap.NewContentsForTesting(bootstrap.ConfigOptionsForTesting{
		Servers: []byte(fmt.Sprintf(`[{
			 "server_uri": %q,
			 "channel_creds": [{"type": "insecure"}]
		 }]`, "non-existent-management-server")),
		Node: []byte(fmt.Sprintf(`{"id": "%s"}`, uuid.New().String())),
		CertificateProviders: map[string]json.RawMessage{
			"cert-provider-instance": json.RawMessage("{}"),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create bootstrap configuration: %v", err)
	}

	testutils.CreateBootstrapFileForTesting(t, bs)
	if cfg := xdsclient.DefaultPool.BootstrapConfigForTesting(); cfg != nil {
		t.Fatalf("DefaultPool.BootstrapConfigForTesting() = %v, want nil", cfg)
	}

	// The pool will attempt to read the env vars only once. Reset the pool's
	// state to make it re-read the env vars during next client creation.
	xdsclient.DefaultPool.UnsetBootstrapConfigForTesting()

	_, closeFunc, err = xdsclient.DefaultPool.NewClient(t.Name(), &estats.UnimplementedMetricsRecorder{}, nil)
	if err != nil {
		t.Fatalf("Failed to create xDS client: %v", err)
	}
	defer func() {
		closeFunc()
		xdsclient.DefaultPool.UnsetBootstrapConfigForTesting()
	}()

	if xdsclient.DefaultPool.BootstrapConfigForTesting() == nil {
		t.Fatalf("DefaultPool.BootstrapConfigForTesting() = nil, want non-nil")
	}
}

// TestNestedXDSChannel tests a scenario where xDS is used to resolve the
// address of the management server, resulting in the creation of two nested xDS
// client channels. The server URI of the first xDS management server uses
// the xDS scheme. To resolve its address, the client creates a second xDS
// channel to a different management server. After retrieving the EDS resource
// for the first management server, the client connects to it to obtain the
// address of the test stub server. The test verifies that an RPC to the test
// stub server succeeds. It also helps detect potential deadlocks during the
// creation and teardown of the two nested xDS channels.
func (s) TestNestedXDSChannel(t *testing.T) {
	lis1, err := testutils.LocalTCPListener()
	if err != nil {
		t.Fatal(err)
	}
	lis2, err := testutils.LocalTCPListener()
	if err != nil {
		t.Fatal(err)
	}

	const managementServerServiceName = "management_server"
	const managementServerAuthority = "management_server_authority"
	managementServerLDSName := fmt.Sprintf(
		"xdstp://%s/envoy.config.listener.v3.Listener/%s",
		managementServerAuthority, managementServerServiceName,
	)
	managementServerRDSName := fmt.Sprintf(
		"xdstp://%s/envoy.config.route.v3.RouteConfiguration/route-%s",
		managementServerAuthority, managementServerServiceName,
	)

	managementServerCDSName := fmt.Sprintf(
		"xdstp://%s/envoy.config.cluster.v3.Cluster/cluster-%s",
		managementServerAuthority, managementServerServiceName,
	)

	managementServerEDSName := fmt.Sprintf(
		"xdstp://%s/envoy.config.endpoint.v3.ClusterLoadAssignment/endpoints-%s",
		managementServerAuthority, managementServerServiceName,
	)

	const serviceName = "my-service-client-side-xds"
	const rdsName = "route-" + serviceName
	const cdsName = "cluster-" + serviceName
	const edsName = "endpoints-" + serviceName

	// Start a management server that stores resources related to the test
	// server.
	managementServer1 := e2e.StartManagementServer(t, e2e.ManagementServerOptions{Listener: lis1})

	// Start a management server that stores resources related to
	// managementServer1.
	managementServer2 := e2e.StartManagementServer(t, e2e.ManagementServerOptions{Listener: lis2})

	server := stubserver.StartTestService(t, nil)
	defer server.Stop()

	// Create a bootstrap configuration where the xDS scheme is used to connect
	// to the default management server. A non-default authority is also
	// specified which has resources for resolving the address of the default
	// management server.
	nodeID := uuid.New().String()
	bootstrapContents, err := bootstrap.NewContentsForTesting(bootstrap.ConfigOptionsForTesting{
		Servers: fmt.Appendf(nil, `[{
			"server_uri": "xds://%s/%s",
			"channel_creds": [{"type": "insecure"}]
		}]`, managementServerAuthority, managementServerServiceName),
		Node: fmt.Appendf(nil, `{"id": "%s"}`, nodeID),
		Authorities: map[string]json.RawMessage{
			managementServerAuthority: fmt.Appendf(nil, `{
				"xds_servers": [{
					"server_uri": %q,
					"channel_creds": [{"type": "insecure"}]
				}]}`, managementServer1.Address),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create bootstrap configuration: %v", err)
	}
	config, err := bootstrap.NewConfigFromContents(bootstrapContents)
	if err != nil {
		t.Fatalf("Failed to parse bootstrap contents: %v", err)
	}
	xdsclient.DefaultPool.SetFallbackBootstrapConfig(config)
	defer func() { xdsclient.DefaultPool.UnsetBootstrapConfigForTesting() }()

	// Update the management server that holds resources for resolving the real
	// management server.
	managementServerResources := e2e.UpdateOptions{
		NodeID:    nodeID,
		Listeners: []*v3listenerpb.Listener{e2e.DefaultClientListener(managementServerLDSName, managementServerRDSName)},
		Routes: []*v3routepb.RouteConfiguration{
			e2e.DefaultRouteConfig(managementServerRDSName, managementServerServiceName, managementServerCDSName),
		},
		Clusters: []*v3clusterpb.Cluster{
			e2e.DefaultCluster(managementServerCDSName, managementServerEDSName, e2e.SecurityLevelNone),
		},
		Endpoints: []*v3endpointpb.ClusterLoadAssignment{
			e2e.DefaultEndpoint(managementServerEDSName, "localhost", []uint32{testutils.ParsePort(t, managementServer2.Address)}),
		},
		SkipValidation: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	if err := managementServer1.Update(ctx, managementServerResources); err != nil {
		t.Fatalf("Failed to update management server with resources: %v, err: %v", managementServerResources, err)
	}

	// Update the management server that resolves the address of the test stub
	// server.
	resouces := e2e.UpdateOptions{
		NodeID:    nodeID,
		Listeners: []*v3listenerpb.Listener{e2e.DefaultClientListener(serviceName, rdsName)},
		Routes: []*v3routepb.RouteConfiguration{
			e2e.DefaultRouteConfig(rdsName, serviceName, cdsName),
		},
		Clusters: []*v3clusterpb.Cluster{
			e2e.DefaultCluster(cdsName, edsName, e2e.SecurityLevelNone),
		},
		Endpoints: []*v3endpointpb.ClusterLoadAssignment{
			e2e.DefaultEndpoint(edsName, "localhost", []uint32{testutils.ParsePort(t, server.Address)}),
		},
		SkipValidation: true,
	}

	if err := managementServer2.Update(ctx, resouces); err != nil {
		t.Fatalf("Failed to update management server with resources: %v, err: %v", resouces, err)
	}

	cc, err := grpc.NewClient("xds:///"+serviceName, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient() failed: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("rpc EmptyCall() failed: %v", err)
	}
}

type testDialOption struct {
	grpc.EmptyDialOption
	name string
}

// Verifies shared-client resolution rules and child dial option propagation
// when multiple callers request an xDS client for the same target name from a
// Pool:
//   - If a client for the target already exists in the pool, the first caller's
//     childDialOptions win and subsequent callers' options are ignored.
//   - When the shared client dials the control plane, the first caller's
//     options and recursive WithChildChannelOptions are applied to the gRPC
//     channel.
//   - Only after all references are closed does creating a new client for that
//     target apply new childDialOptions.
func (s) TestPool_SharedClientChildDialOptions(t *testing.T) {
	bs, err := bootstrap.NewContentsForTesting(bootstrap.ConfigOptionsForTesting{
		Servers: []byte(`[{
			"server_uri": "passthrough:///non-existent-management-server:443",
			"channel_creds": [{"type": "insecure"}]
		}]`),
		Node: []byte(fmt.Sprintf(`{"id": "%s"}`, uuid.New().String())),
	})
	if err != nil {
		t.Fatalf("Failed to create bootstrap configuration: %v", err)
	}
	config, err := bootstrap.NewConfigFromContents(bs)
	if err != nil {
		t.Fatalf("Failed to parse bootstrap configuration: %v", err)
	}
	pool := xdsclient.NewPool(config)

	type streamInterceptCall struct {
		caller string
		cc     *grpc.ClientConn
	}
	streamCh := testutils.NewChannel()
	makeCallerOpts := func(name string) []grpc.DialOption {
		return []grpc.DialOption{
			&testDialOption{name: name},
			grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				streamCh.Replace(streamInterceptCall{caller: name, cc: cc})
				return streamer(ctx, desc, cc, method, opts...)
			}),
		}
	}

	const (
		caller1Name = "caller1_opt"
		caller2Name = "caller2_opt"
		targetName  = "test-shared-client-target"
	)

	caller1Opts := makeCallerOpts(caller1Name)
	caller2Opts := makeCallerOpts(caller2Name)
	caller3Opts := makeCallerOpts("caller3_opt")

	// Caller 1 creates the xDS client with caller1Opts.
	client1, close1, err := pool.NewClient(targetName, &estats.UnimplementedMetricsRecorder{}, caller1Opts)
	if err != nil {
		t.Fatalf("Failed to create client for caller 1 via pool.NewClient(): %v", err)
	}

	// Caller 2 requests the same target with caller2Opts; must reuse client1 and
	// ignore caller2Opts.
	client2, close2, err := pool.NewClient(targetName, &estats.UnimplementedMetricsRecorder{}, caller2Opts)
	if err != nil {
		t.Fatalf("Failed to create client for caller 2 via pool.NewClient(): %v", err)
	}

	// Caller 3 requests the same target via NewClientWithConfig with caller3Opts;
	// must reuse client1 and ignore caller3Opts.
	client3, close3, err := pool.NewClientWithConfig(targetName, &estats.UnimplementedMetricsRecorder{}, config, caller3Opts)
	if err != nil {
		t.Fatalf("Failed to create client for caller 3 via pool.NewClientWithConfig(): %v", err)
	}

	if client1 != client2 || client1 != client3 {
		t.Fatalf("Expected shared client instance across callers, got client1=%p, client2=%p, client3=%p", client1, client2, client3)
	}

	extractNames := func(opts []grpc.DialOption) []string {
		var names []string
		for _, opt := range opts {
			if tdOpt, ok := opt.(*testDialOption); ok {
				names = append(names, tdOpt.name)
			}
		}
		return names
	}

	// Start load reporting on client1 to trigger gRPC channel and stream
	// creation to the control plane.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	_, stopLoadReport := client1.ReportLoad(config.XDSServers()[0])

	val, err := streamCh.Receive(ctx)
	if err != nil {
		t.Fatalf("Timeout waiting for control plane stream creation: %v", err)
	}
	call := val.(streamInterceptCall)

	// Verify that the channel to the control plane applied caller 1's stream
	// interceptor and NOT caller 2 or 3's options.
	if call.caller != caller1Name {
		t.Errorf("control plane stream interceptor caller = %q, want %q", call.caller, caller1Name)
	}

	// Verify recursive option insertion on the created control plane ClientConn.
	//
	// TODO: Remove this ChildDialOptionsFromClientConn workaround once
	// ClientConn and the xDS resolver propagate child dial options to resolvers
	// and nested xDS channels.
	getChildOpts := grpcinternal.ChildDialOptionsFromClientConn.(func(*grpc.ClientConn) []grpc.DialOption)
	gotCCChildOptNames := extractNames(getChildOpts(call.cc))
	if diff := cmp.Diff([]string{caller1Name}, gotCCChildOptNames); diff != "" {
		t.Errorf("recursive ChildDialOptions on control plane ClientConn mismatch (-want +got):\n%s", diff)
	}

	stopCtx, stopCancel := context.WithCancel(context.Background())
	stopCancel()
	stopLoadReport(stopCtx)

	// Release caller 1 and caller 2 references. Since caller 3 still holds a
	// reference, the client must remain alive in the pool.
	close1()
	close2()

	client4, close4, err := pool.NewClient(targetName, &estats.UnimplementedMetricsRecorder{}, caller2Opts)
	if err != nil {
		t.Fatalf("Failed to create client via pool.NewClient() while refcount > 0: %v", err)
	}
	if client4 != client1 {
		t.Fatalf("Expected existing client %p while refcount > 0, got %p", client1, client4)
	}
	close4()

	// Release the last reference (caller 3). Now the client should be closed
	// and removed from the pool.
	close3()

	// Creating a client for targetName now should create a new instance with
	// caller 2's options winning.
	clientNew, closeNew, err := pool.NewClient(targetName, &estats.UnimplementedMetricsRecorder{}, caller2Opts)
	if err != nil {
		t.Fatalf("Failed to create client via pool.NewClient() after full close: %v", err)
	}
	defer closeNew()

	_, stopNewLoadReport := clientNew.ReportLoad(config.XDSServers()[0])
	defer stopNewLoadReport(stopCtx)

	val, err = streamCh.Receive(ctx)
	if err != nil {
		t.Fatalf("Timeout waiting for new client control plane stream creation: %v", err)
	}
	newCall := val.(streamInterceptCall)
	if newCall.caller != caller2Name {
		t.Errorf("new client control plane stream interceptor caller = %q, want %q", newCall.caller, caller2Name)
	}
}
