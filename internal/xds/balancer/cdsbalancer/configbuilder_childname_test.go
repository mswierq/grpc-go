/*
 *
 * Copyright 2022 gRPC authors.
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
 */

package cdsbalancer

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/internal/xds/clients"
	"google.golang.org/grpc/internal/xds/xdsclient/xdsresource"
)

func (s) Test_nameGenerator_generate(t *testing.T) {
	tests := []struct {
		name        string
		clusterName string
		steps       []struct {
			input [][]xdsresource.Locality
			want  []string
		}
	}{
		{
			name:        "init, two new priorities",
			clusterName: "cluster-3",
			steps: []struct {
				input [][]xdsresource.Locality
				want  []string
			}{
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}},
						{{ID: clients.Locality{Zone: "L1"}}},
					},
					want: []string{"{cluster=cluster-3, child_number=0}", "{cluster=cluster-3, child_number=1}"},
				},
			},
		},
		{
			name:        "one new priority",
			clusterName: "cluster-1",
			steps: []struct {
				input [][]xdsresource.Locality
				want  []string
			}{
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}},
					},
					want: []string{"{cluster=cluster-1, child_number=0}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}},
						{{ID: clients.Locality{Zone: "L1"}}},
					},
					want: []string{"{cluster=cluster-1, child_number=0}", "{cluster=cluster-1, child_number=1}"},
				},
			},
		},
		{
			name:        "merge two priorities",
			clusterName: "cluster-4",
			steps: []struct {
				input [][]xdsresource.Locality
				want  []string
			}{
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}},
						{{ID: clients.Locality{Zone: "L1"}}},
						{{ID: clients.Locality{Zone: "L2"}}},
					},
					want: []string{"{cluster=cluster-4, child_number=0}", "{cluster=cluster-4, child_number=1}", "{cluster=cluster-4, child_number=2}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}, {ID: clients.Locality{Zone: "L1"}}},
						{{ID: clients.Locality{Zone: "L2"}}},
					},
					want: []string{"{cluster=cluster-4, child_number=0}", "{cluster=cluster-4, child_number=2}"},
				},
			},
		},
		{
			name:        "swap two priorities",
			clusterName: "cluster-0",
			steps: []struct {
				input [][]xdsresource.Locality
				want  []string
			}{
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}},
						{{ID: clients.Locality{Zone: "L1"}}},
						{{ID: clients.Locality{Zone: "L2"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}", "{cluster=cluster-0, child_number=2}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L1"}}},
						{{ID: clients.Locality{Zone: "L0"}}},
						{{ID: clients.Locality{Zone: "L2"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=1}", "{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=2}"},
				},
			},
		},
		{
			name:        "split priority",
			clusterName: "cluster-0",
			steps: []struct {
				input [][]xdsresource.Locality
				want  []string
			}{
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}, {ID: clients.Locality{Zone: "L1"}}},
						{{ID: clients.Locality{Zone: "L2"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}},
						{{ID: clients.Locality{Zone: "L1"}}},
						{{ID: clients.Locality{Zone: "L2"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=2}", "{cluster=cluster-0, child_number=1}"},
				},
			},
		},
		{
			name:        "priority index preference",
			clusterName: "cluster-0",
			steps: []struct {
				input [][]xdsresource.Locality
				want  []string
			}{
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}},
						{{ID: clients.Locality{Zone: "L1"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L1"}}, {ID: clients.Locality{Zone: "L0"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}"},
				},
			},
		},
		{
			name:        "merge partial",
			clusterName: "cluster-0",
			steps: []struct {
				input [][]xdsresource.Locality
				want  []string
			}{
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}, {ID: clients.Locality{Zone: "L1"}}},
						{{ID: clients.Locality{Zone: "L2"}}, {ID: clients.Locality{Zone: "L3"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}, {ID: clients.Locality{Zone: "L1"}}, {ID: clients.Locality{Zone: "L2"}}},
						{{ID: clients.Locality{Zone: "L3"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}, {ID: clients.Locality{Zone: "L1"}}},
						{{ID: clients.Locality{Zone: "L2"}}, {ID: clients.Locality{Zone: "L3"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
			},
		},
		{
			name:        "swap shift",
			clusterName: "cluster-0",
			steps: []struct {
				input [][]xdsresource.Locality
				want  []string
			}{
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}, {ID: clients.Locality{Zone: "L1"}}},
						{{ID: clients.Locality{Zone: "L2"}}, {ID: clients.Locality{Zone: "L3"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L2"}}},
						{{ID: clients.Locality{Zone: "L0"}}, {ID: clients.Locality{Zone: "L1"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=1}", "{cluster=cluster-0, child_number=0}"},
				},
			},
		},
		{
			name:        "replace priority",
			clusterName: "cluster-0",
			steps: []struct {
				input [][]xdsresource.Locality
				want  []string
			}{
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}, {ID: clients.Locality{Zone: "L1"}}},
						{{ID: clients.Locality{Zone: "L2"}}, {ID: clients.Locality{Zone: "L3"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}, {ID: clients.Locality{Zone: "L1"}}},
						{{ID: clients.Locality{Zone: "L5"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=2}"},
				},
			},
		},
		{
			name:        "reordered merge",
			clusterName: "cluster-0",
			steps: []struct {
				input [][]xdsresource.Locality
				want  []string
			}{
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L1"}}, {ID: clients.Locality{Zone: "L2"}}},
						{{ID: clients.Locality{Zone: "L0"}}, {ID: clients.Locality{Zone: "L3"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}, {ID: clients.Locality{Zone: "L1"}}, {ID: clients.Locality{Zone: "L2"}}},
						{{ID: clients.Locality{Zone: "L3"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L1"}}, {ID: clients.Locality{Zone: "L2"}}},
						{{ID: clients.Locality{Zone: "L0"}}, {ID: clients.Locality{Zone: "L3"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
			},
		},
		{
			name:        "three-step shift stability",
			clusterName: "cluster-0",
			steps: []struct {
				input [][]xdsresource.Locality
				want  []string
			}{
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}},
						{{ID: clients.Locality{Zone: "L1"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L2"}}},
						{{ID: clients.Locality{Zone: "L0"}}},
						{{ID: clients.Locality{Zone: "L1"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=2}", "{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
				{
					input: [][]xdsresource.Locality{
						{{ID: clients.Locality{Zone: "L0"}}},
						{{ID: clients.Locality{Zone: "L1"}}},
					},
					want: []string{"{cluster=cluster-0, child_number=0}", "{cluster=cluster-0, child_number=1}"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ng := newNameGenerator(tt.clusterName)
			for i, step := range tt.steps {
				got := ng.generate(step.input)
				if diff := cmp.Diff(got, step.want); diff != "" {
					t.Errorf("step %d: generate() = got: %v, want: %v, diff (-got +want): %s", i, got, step.want, diff)
				}
			}
		})
	}
}
