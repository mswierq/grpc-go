# A75: xDS Aggregate Cluster Behaviour Fixes

## Background

This document describes what has to be done to implement [A75](https://github.com/grpc/proposal/blob/master/A75-xds-aggregate-cluster-behavior-fixes.md) for Go implementation to be aligned with C-Core.
The expected result should be a set of sized tasks that will cover all the required fixes and changes.

## grpc-go scope of work

### Impacted features

- [A37](https://github.com/grpc/proposal/blob/master/A37-xds-aggregate-and-logical-dns-clusters.md) Aggregate cluster implementation assumptions are wrong
- [A50](https://github.com/grpc/proposal/blob/master/A50-xds-outlier-detection.md) Outlier detection 
