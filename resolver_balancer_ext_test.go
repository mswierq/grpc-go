/*
 *
 * Copyright 2023 gRPC authors.
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

package grpc_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/endpointsharding"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/internal"
	"google.golang.org/grpc/internal/balancer/gracefulswitch"
	"google.golang.org/grpc/internal/balancer/stub"
	"google.golang.org/grpc/internal/balancergroup"
	"google.golang.org/grpc/internal/channelz"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

// TestResolverBalancerInteraction tests:
// 1. resolver.Builder.Build() ->
// 2. resolver.ClientConn.UpdateState() ->
// 3. balancer.Balancer.UpdateClientConnState() ->
// 4. balancer.ClientConn.ResolveNow() ->
// 5. resolver.Resolver.ResolveNow() ->
func (s) TestResolverBalancerInteraction(t *testing.T) {
	name := strings.ReplaceAll(strings.ToLower(t.Name()), "/", "")
	bf := stub.BalancerFuncs{
		UpdateClientConnState: func(bd *stub.BalancerData, _ balancer.ClientConnState) error {
			bd.ClientConn.ResolveNow(resolver.ResolveNowOptions{})
			return nil
		},
	}
	stub.Register(name, bf)

	rb := manual.NewBuilderWithScheme(name)
	rb.BuildCallback = func(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) {
		sc := cc.ParseServiceConfig(`{"loadBalancingConfig": [{"` + name + `":{}}]}`)
		cc.UpdateState(resolver.State{
			Addresses:     []resolver.Address{{Addr: "test"}},
			ServiceConfig: sc,
		})
	}
	rnCh := make(chan struct{})
	rb.ResolveNowCallback = func(resolver.ResolveNowOptions) { close(rnCh) }
	resolver.Register(rb)

	cc, err := grpc.NewClient(name+":///", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient error: %v", err)
	}
	defer cc.Close()
	cc.Connect()
	select {
	case <-rnCh:
	case <-time.After(defaultTestTimeout):
		t.Fatalf("timed out waiting for resolver.ResolveNow")
	}
}

type resolverBuilderWithErr struct {
	resolver.Resolver
	errCh  <-chan error
	scheme string
}

func (b *resolverBuilderWithErr) Build(_ resolver.Target, _ resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	if err := <-b.errCh; err != nil {
		return nil, err
	}
	return b, nil
}

func (b *resolverBuilderWithErr) Scheme() string {
	return b.scheme
}

func (b *resolverBuilderWithErr) Close() {}

// TestResolverBuildFailure tests:
// 1. resolver.Builder.Build() passes.
// 2. Channel enters idle mode.
// 3. An RPC happens.
// 4. resolver.Builder.Build() fails.
func (s) TestResolverBuildFailure(t *testing.T) {
	enterIdle := internal.EnterIdleModeForTesting.(func(*grpc.ClientConn))
	name := strings.ReplaceAll(strings.ToLower(t.Name()), "/", "")
	resErrCh := make(chan error, 1)
	resolver.Register(&resolverBuilderWithErr{errCh: resErrCh, scheme: name})

	resErrCh <- nil
	cc, err := grpc.NewClient(name+":///", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient error: %v", err)
	}
	defer cc.Close()
	cc.Connect()
	enterIdle(cc)
	const errStr = "test error from resolver builder"
	t.Log("pushing res err")
	resErrCh <- errors.New(errStr)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := cc.Invoke(ctx, "/a/b", nil, nil); err == nil || !strings.Contains(err.Error(), errStr) {
		t.Fatalf("Invoke = %v; want %v", err, errStr)
	}
}

// Tests the case where the resolver reports an error to the channel before
// reporting an update. Verifies that the channel eventually moves to
// TransientFailure and subsequent RPCs returns the error reported by the
// resolver to the user.
func (s) TestResolverReportError(t *testing.T) {
	const resolverErr = "test resolver error"
	r := manual.NewBuilderWithScheme("whatever")
	r.BuildCallback = func(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) {
		cc.ReportError(errors.New(resolverErr))
	}

	cc, err := grpc.NewClient(r.Scheme()+":///", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(r))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer cc.Close()
	cc.Connect()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	testutils.AwaitState(ctx, t, cc, connectivity.TransientFailure)

	client := testgrpc.NewTestServiceClient(cc)
	for range 5 {
		_, err = client.EmptyCall(ctx, &testpb.Empty{})
		if code := status.Code(err); code != codes.Unavailable {
			t.Fatalf("EmptyCall() = %v, want %v", err, codes.Unavailable)
		}
		if err == nil || !strings.Contains(err.Error(), resolverErr) {
			t.Fatalf("EmptyCall() = %q, want %q", err, resolverErr)
		}
	}
}

// TestEnterIdleDuringResolverUpdateState tests a scenario that used to deadlock
// while calling UpdateState at the same time as the resolver being closed while
// the channel enters idle mode.
func (s) TestEnterIdleDuringResolverUpdateState(t *testing.T) {
	enterIdle := internal.EnterIdleModeForTesting.(func(*grpc.ClientConn))
	name := strings.ReplaceAll(strings.ToLower(t.Name()), "/", "")

	// Create a manual resolver that spams UpdateState calls until it is closed.
	rb := manual.NewBuilderWithScheme(name)
	var cancel context.CancelFunc
	rb.BuildCallback = func(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) {
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		go func() {
			for ctx.Err() == nil {
				cc.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: "test"}}})
			}
		}()
	}
	rb.CloseCallback = func() {
		cancel()
	}
	resolver.Register(rb)

	cc, err := grpc.NewClient(name+":///", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient error: %v", err)
	}
	defer cc.Close()

	// Enter/exit idle mode repeatedly.
	for i := 0; i < 2000; i++ {
		// Start a timer so we panic out of the deadlock and can see all the
		// stack traces to debug the problem.
		p := time.AfterFunc(time.Second, func() {
			buf := make([]byte, 8192)
			buf = buf[0:runtime.Stack(buf, true)]
			t.Error("Timed out waiting for enterIdle")
			panic(fmt.Sprint("Stack trace:\n", string(buf)))
		})
		enterIdle(cc)
		p.Stop()
		cc.Connect()
	}
}

// TestEnterIdleDuringBalancerUpdateState tests calling UpdateState at the same
// time as the balancer being closed while the channel enters idle mode.
func (s) TestEnterIdleDuringBalancerUpdateState(t *testing.T) {
	enterIdle := internal.EnterIdleModeForTesting.(func(*grpc.ClientConn))
	name := strings.ReplaceAll(strings.ToLower(t.Name()), "/", "")

	// Create a balancer that calls UpdateState once asynchronously, attempting
	// to make the channel appear ready even after entering idle.
	bf := stub.BalancerFuncs{
		UpdateClientConnState: func(bd *stub.BalancerData, _ balancer.ClientConnState) error {
			go func() {
				bd.ClientConn.UpdateState(balancer.State{ConnectivityState: connectivity.Ready})
			}()
			return nil
		},
	}
	stub.Register(name, bf)

	rb := manual.NewBuilderWithScheme(name)
	rb.BuildCallback = func(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) {
		cc.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: "test"}}})
	}
	resolver.Register(rb)

	cc, err := grpc.NewClient(
		name+":///",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"`+name+`":{}}]}`))
	if err != nil {
		t.Fatalf("grpc.NewClient error: %v", err)
	}
	defer cc.Close()

	// Enter/exit idle mode repeatedly.
	for i := 0; i < 2000; i++ {
		enterIdle(cc)
		if got, want := cc.GetState(), connectivity.Idle; got != want {
			t.Fatalf("cc state = %v; want %v", got, want)
		}
		cc.Connect()
	}
}

// TestEnterIdleDuringBalancerNewSubConn tests calling NewSubConn at the same
// time as the balancer being closed while the channel enters idle mode.
func (s) TestEnterIdleDuringBalancerNewSubConn(t *testing.T) {
	channelz.TurnOn()
	defer internal.ChannelzTurnOffForTesting()
	enterIdle := internal.EnterIdleModeForTesting.(func(*grpc.ClientConn))
	name := strings.ReplaceAll(strings.ToLower(t.Name()), "/", "")

	// Create a balancer that calls NewSubConn once asynchronously, attempting
	// to create a subchannel after going idle.
	bf := stub.BalancerFuncs{
		UpdateClientConnState: func(bd *stub.BalancerData, _ balancer.ClientConnState) error {
			go func() {
				bd.ClientConn.NewSubConn([]resolver.Address{{Addr: "test"}}, balancer.NewSubConnOptions{})
			}()
			return nil
		},
	}
	stub.Register(name, bf)

	rb := manual.NewBuilderWithScheme(name)
	rb.BuildCallback = func(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) {
		cc.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: "test"}}})
	}
	resolver.Register(rb)

	cc, err := grpc.NewClient(
		name+":///",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"`+name+`":{}}]}`))
	if err != nil {
		t.Fatalf("grpc.NewClient error: %v", err)
	}
	defer cc.Close()

	// Enter/exit idle mode repeatedly.
	for i := 0; i < 2000; i++ {
		enterIdle(cc)
		tcs, _ := channelz.GetTopChannels(0, 0)
		if len(tcs) != 1 {
			t.Fatalf("Found channels: %v; expected 1 entry", tcs)
		}
		if got := tcs[0].SubChans(); len(got) != 0 {
			t.Fatalf("Found subchannels: %v; expected 0 entries", got)
		}
		cc.Connect()
	}
}

type testChildDialOption struct {
	grpc.EmptyDialOption
	val string
}

// TestChildDialOptions_ResolverAndBalancer verifies that resolvers and
// balancers receive the configured child dial options upon Build().
func (s) TestChildDialOptions_ResolverAndBalancer(t *testing.T) {
	name := strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(t.Name()), "/", ""), "_", "")
	childOpt := testChildDialOption{val: "test-child-dial-option"}

	resolverBuildOptsCh := make(chan resolver.BuildOptions, 1)
	rb := manual.NewBuilderWithScheme(name)
	rb.BuildCallback = func(_ resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) {
		resolverBuildOptsCh <- opts
		cc.UpdateState(resolver.State{
			Addresses: []resolver.Address{{Addr: "test"}},
		})
	}
	resolver.Register(rb)

	balancerBuildOptsCh := make(chan balancer.BuildOptions, 1)
	bf := stub.BalancerFuncs{
		Init: func(bd *stub.BalancerData) {
			balancerBuildOptsCh <- bd.BuildOptions
		},
	}
	stub.Register(name, bf)

	cc, err := grpc.NewClient(
		name+":///",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChildChannelOptions(childOpt),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"`+name+`":{}}]}`),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
	}
	defer cc.Close()
	cc.Connect()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	var resolverOpts resolver.BuildOptions
	select {
	case resolverOpts = <-resolverBuildOptsCh:
	case <-ctx.Done():
		t.Fatalf("Timed out waiting for resolver.Build to be called")
	}

	if len(resolverOpts.ChildDialOptions) != 1 {
		t.Fatalf("Resolver received ChildDialOptions length %d, want 1", len(resolverOpts.ChildDialOptions))
	}
	if resolverOpts.ChildDialOptions[0] != childOpt {
		t.Fatalf("Resolver received ChildDialOptions[0] = %v, want %v", resolverOpts.ChildDialOptions[0], childOpt)
	}

	var balancerOpts balancer.BuildOptions
	select {
	case balancerOpts = <-balancerBuildOptsCh:
	case <-ctx.Done():
		t.Fatalf("Timed out waiting for balancer.Build to be called")
	}

	if len(balancerOpts.ChildDialOptions) != 1 {
		t.Fatalf("Balancer received ChildDialOptions length %d, want 1", len(balancerOpts.ChildDialOptions))
	}
	if balancerOpts.ChildDialOptions[0] != childOpt {
		t.Fatalf("Balancer received ChildDialOptions[0] = %v, want %v", balancerOpts.ChildDialOptions[0], childOpt)
	}
}

// TestChildDialOptions_BalancerDecorators verifies that balancer combinators
// (gracefulswitch, balancergroup, and endpointsharding) forward child dial
// options to their child balancers.
func (s) TestChildDialOptions_BalancerDecorators(t *testing.T) {
	childOpt := testChildDialOption{val: "test-child-dial-option"}
	bOpts := balancer.BuildOptions{
		DialCreds:        insecure.NewCredentials(),
		ChildDialOptions: []any{childOpt},
	}

	tests := []struct {
		name  string
		setup func(t *testing.T, cc balancer.ClientConn, bOpts balancer.BuildOptions, childBuilderName string)
	}{
		{
			name: "gracefulswitch",
			setup: func(t *testing.T, cc balancer.ClientConn, bOpts balancer.BuildOptions, childBuilderName string) {
				gsb := gracefulswitch.NewBalancer(cc, bOpts)
				t.Cleanup(gsb.Close)

				if err := gsb.SwitchTo(balancer.Get(childBuilderName)); err != nil {
					t.Fatalf("gsb.SwitchTo failed: %v", err)
				}
			},
		},
		{
			name: "balancergroup",
			setup: func(t *testing.T, cc balancer.ClientConn, bOpts balancer.BuildOptions, childBuilderName string) {
				bg := balancergroup.New(balancergroup.Options{
					CC:        cc,
					BuildOpts: bOpts,
				})
				t.Cleanup(bg.Close)

				bg.Add("child", balancer.Get(childBuilderName))
				if err := bg.UpdateClientConnState("child", balancer.ClientConnState{}); err != nil {
					t.Fatalf("bg.UpdateClientConnState failed: %v", err)
				}
			},
		},
		{
			name: "endpointsharding",
			setup: func(t *testing.T, cc balancer.ClientConn, bOpts balancer.BuildOptions, childBuilderName string) {
				es := endpointsharding.NewBalancer(cc, bOpts, balancer.Get(childBuilderName).Build, endpointsharding.Options{})
				t.Cleanup(es.Close)

				if err := es.UpdateClientConnState(balancer.ClientConnState{
					ResolverState: resolver.State{
						Endpoints: []resolver.Endpoint{{Addresses: []resolver.Address{{Addr: "127.0.0.1:1234"}}}},
					},
				}); err != nil {
					t.Fatalf("es.UpdateClientConnState failed: %v", err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			childBuilderName := strings.ReplaceAll(strings.ToLower(t.Name()), "/", "")
			childOptsCh := make(chan balancer.BuildOptions, 1)
			stub.Register(childBuilderName, stub.BalancerFuncs{
				Init: func(bd *stub.BalancerData) {
					childOptsCh <- bd.BuildOptions
				},
			})

			cc := testutils.NewBalancerClientConn(t)
			tc.setup(t, cc, bOpts, childBuilderName)

			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			defer cancel()

			var got balancer.BuildOptions
			select {
			case got = <-childOptsCh:
			case <-ctx.Done():
				t.Fatalf("Timed out waiting for child balancer to be built")
			}

			if len(got.ChildDialOptions) != 1 {
				t.Fatalf("Child received ChildDialOptions length %d, want 1", len(got.ChildDialOptions))
			}
			if got.ChildDialOptions[0] != childOpt {
				t.Fatalf("Child received ChildDialOptions[0] = %v, want %v", got.ChildDialOptions[0], childOpt)
			}
		})
	}
}
