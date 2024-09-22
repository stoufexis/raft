# raft

This repository contains a generic implementation of the [raft consensus algorithm](https://raft.github.io/),
along with an application of this implementation in a distributed, replicated key-value store.

Raft was implemented using cats-effect 3 and fs2. [Diego Ongaro's PhD dissertation](https://github.com/ongardie/dissertation) was used as the main reference.

Note that currently this is still a WIP.

## Raft features implemented

This project aimed to build a basic raft system which could be extended in the future to fit real-world use cases.

| Raft features | Implemented |
|----------|:-------------:|
| Leader election | :white_check_mark: |
| Log replication | :white_check_mark: |
| Persisted state | :white_check_mark: |
| Leadership transfer extension | :x: |
| Dynamic cluster membership changes | :x: |
| Log compaction | :x: |
| Redirecting to leader | :white_check_mark: |
| Leader self demotion on partition | :white_check_mark: |
| Linearizable semantics | :white_check_mark: * |

\* Linearizability was implemented in a simpler way than how it is described in Diego's dissertation.
- There is no distinguishing between read and write commands
- exactly-once semantics are implemented by rejecting duplicated commands and providing a chance for user code to return something informative to the client.

The implemented features coincide with chapters 3 (except 3.10) and 6 of Diego's dissertation.

## key-value store

The replicated key-value store implementation aims to be extremely simple and easy to reason about; a lot of design choices were made in favor of simplicity and ease of demonstration instead of performance. Sqlite is used for persistence and http is used for rpc. Clients are served via http as well.

Three commands are supported:
* Get: recovers the values of a set of keys
* Update: sets the values of a set of keys
* TransactionUpdate: sets the values of a set of keys, as long as a set of reference keys have not changed in value since a previous Get. If the reference keys have changed the client is prompted to retry. This effectively implements optimistic locking and thus serves as a simple transaction mechanism.

A docker image of a key-value store node can be built via `sbt> kvstore / Docker / publishLocal` and a docker compose configuration is provided for setting up a cluster of 3 nodes, along with an nginx proxy.

## The future
This will most likely become my university thesis, in which case there will be an accompanying paper
