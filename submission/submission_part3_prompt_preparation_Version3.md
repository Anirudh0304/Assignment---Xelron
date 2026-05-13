# Part 3 - Prompt Preparation  
Repository: https://github.com/aio-libs/aiokafka  
Chosen PR: https://github.com/aio-libs/aiokafka/pull/196  
PR Title: Added separate socket groups to client

---

## 3.1.1 Repository Context

`aiokafka` is an asynchronous Python client library for Apache Kafka. It allows Python applications using `asyncio` to communicate with Kafka brokers through producer and consumer APIs. The repository mainly focuses on enabling non-blocking Kafka operations in Python applications, where messages can be produced to topics and consumed from topics without blocking the event loop.

The intended users of this repository are Python developers who build event-driven systems, streaming applications, backend services, data pipelines, and microservices that depend on Kafka for message transport. Instead of using a synchronous Kafka client, users of `aiokafka` can integrate Kafka communication into applications that already use asynchronous programming patterns.

The problem domain addressed by this repository is distributed messaging and stream processing. Kafka is commonly used for high-throughput event pipelines, log processing, analytics systems, and service-to-service communication. Since Kafka clients need to manage broker connections, metadata updates, partition leadership, producer acknowledgements, and consumer group coordination, the library must handle both networking and protocol-level details reliably.

A key design constraint of `aiokafka` is that it must follow Kafka’s request-response protocol while also fitting into Python’s `asyncio` model. This means the client needs to manage TCP connections, schedule requests without blocking the event loop, and coordinate background tasks such as heartbeats, offset commits, metadata refreshes, and fetch requests. Because Kafka requests on a single connection are handled synchronously, connection management is especially important for avoiding delays between different types of client operations.

---

## 3.1.2 Pull Request Description

PR #196 introduces separate socket groups inside the `AIOKafkaClient` to reduce blocking between different categories of Kafka requests. Before this change, the client effectively reused one connection per broker node. This created a head-of-line blocking problem because Kafka processes requests on a connection in order. If a long-running request such as a consumer fetch was waiting for data, another request to the same broker could be delayed behind it even if that second request was unrelated.

This was especially problematic for consumer group coordination. Operations such as heartbeats, joins, syncs, offset commits, offset fetches, and leave-group requests need to reach the group coordinator quickly. If the coordinator broker was also handling a long-poll fetch on the same connection, coordination traffic could be delayed. That delay could affect consumer responsiveness and make commits or heartbeats slower than expected.

The PR solves this by introducing a `ConnectionGroup` concept. Instead of storing connections only by `node_id`, the client stores them using a combined key such as `(node_id, group)`. The default group is used for normal broker traffic, while a separate coordination group is used for group coordinator requests. The client methods such as `_get_conn`, `ready`, and `send` are updated to accept a `group` parameter while preserving the previous default behavior when no group is specified.

The new behavior allows long-poll fetches and coordination requests to use separate sockets even when they target the same broker. As a result, a long fetch request in the default group should not block a commit or heartbeat request sent through the coordination group.

---

## 3.1.3 Acceptance Criteria

✓ When `AIOKafkaClient.send(node_id, request)` is called without a `group` argument, the client should behave the same as before and use the default connection group.

✓ When `_get_conn(node_id, group=ConnectionGroup.DEFAULT)` and `_get_conn(node_id, group=ConnectionGroup.COORDINATION)` are called for the same broker, the client should create or return two different connection objects.

✓ When a long-poll fetch request is active on the default connection group, a coordinator request sent through the coordination group should complete without waiting for the fetch request to finish.

✓ Group coordination operations such as heartbeat, join group, sync group, offset commit, offset fetch, and leave group should send requests using `ConnectionGroup.COORDINATION`.

✓ The internal `_conns` cache should use `(node_id, group)` as the key instead of only `node_id`.

✓ Existing behavior should remain backward compatible for call sites that do not explicitly pass a connection group.

✓ Bootstrap connection handling should continue to work correctly and should not conflict with normal broker node connections.

✓ Updated tests in `tests/test_client.py` and `tests/test_consumer.py` should pass after the implementation.

---

## 3.1.4 Edge Cases

1. **Connection cleanup per group**  
   If a connection for `(node_id, ConnectionGroup.DEFAULT)` is closed, times out, or fails, the corresponding coordination connection for the same node should not be removed unless it also fails.

2. **Bootstrap connection handling**  
   During initial metadata discovery, the bootstrap connection should still be created, used, and cleaned up correctly even after the `_conns` key format changes to include the connection group.

3. **Concurrent connection creation**  
   If multiple coroutines request the same `(node_id, group)` connection at the same time, the client should not create duplicate connections for the same key.

4. **Avoiding unbounded socket creation**  
   The implementation should only use known connection groups such as `DEFAULT` and `COORDINATION`. It should not accidentally allow arbitrary group values that could create too many sockets.

5. **Backward compatibility with default behavior**  
   Any existing client code that calls `send`, `ready`, or `_get_conn` without a group argument should continue to work as it did before.

---

## 3.1.5 Initial Prompt

You are working on the `aio-libs/aiokafka` repository, an asynchronous Python client for Apache Kafka built on top of `asyncio`. The goal is to implement the behavior from PR #196, which adds separate socket groups to the client in order to reduce head-of-line blocking between normal Kafka traffic and consumer group coordination traffic.

Currently, the client effectively maintains one connection per broker node. This can cause a problem because Kafka requests on a single connection are processed synchronously. For example, a long-poll fetch request may stay in progress while waiting for records. If another request, such as an offset commit or heartbeat, needs to go to the same broker, it may be blocked behind the fetch request. This is especially harmful when the broker is also the consumer group coordinator.

Implement a connection grouping mechanism in `AIOKafkaClient`. Add a `ConnectionGroup` definition with at least two groups: `DEFAULT` and `COORDINATION`. Update the internal connection cache so that connections are stored by `(node_id, group)` instead of only `node_id`. The default group should preserve existing behavior for all calls that do not explicitly provide a group.

Update the client methods `_get_conn`, `ready`, and `send` so they accept an optional keyword-only `group` parameter. The default value should be `ConnectionGroup.DEFAULT`. When a group is provided, the method should retrieve or create the connection for that specific `(node_id, group)` pair.

Update the group coordination logic so that coordinator-related requests use `ConnectionGroup.COORDINATION`. This includes heartbeat requests, join group, sync group, offset commit, offset fetch, leave group, and other requests that communicate with the consumer group coordinator.

The implementation should satisfy the following acceptance criteria: normal calls without a group should continue to work as before; default and coordination groups should use separate connections for the same broker; long-poll fetches on the default group should not block coordinator requests on the coordination group; and all affected tests should pass.

Also handle important edge cases. Connection cleanup should apply only to the specific `(node_id, group)` entry that failed. Bootstrap connection behavior should remain correct during metadata discovery and cleanup. Concurrent calls should not create duplicate connections for the same key. The implementation should avoid creating unlimited socket groups and should remain backward compatible with existing client usage.

Update or add tests in `tests/test_client.py` and `tests/test_consumer.py`. The tests should verify that requests on the same connection group are still serialized, requests on different groups use separate connections, and consumer commits are not blocked by long-poll fetches when coordination traffic uses the coordination group.
