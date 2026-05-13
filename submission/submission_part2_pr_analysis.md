# Part 2 - Pull Request Analysis (Task 2.1)
Repository: https://github.com/aio-libs/aiokafka

## PR 1: #196 - Added separate socket groups to client
PR Link: https://github.com/aio-libs/aiokafka/pull/196

### PR Summary
This PR fixes a performance and correctness issue caused by using a single TCP connection per Kafka broker for *all* request types. Because the Kafka protocol is synchronous per connection, a long-polling fetch request (e.g., a consumer fetch waiting up to ~500ms) can block unrelated requests that need to reach the same broker. A common example is consumer offset commits going to the group coordinator; if that coordinator happens to be the same broker handling the long poll, commits get delayed behind the fetch. The PR introduces the idea of “connection groups” so coordination traffic can use a separate connection from general traffic. This reduces head-of-line blocking and keeps group coordination responsive even during long-poll fetches.

### Technical Changes (files/components modified)
- `aiokafka/client.py`
  - Introduces `ConnectionGroup` concept
  - Changes connection cache key from `node_id` → `(node_id, group)`
  - Extends `ready()` / `send()` / `_get_conn()` to accept `group=...`
- `aiokafka/group_coordinator.py`
  - Routes coordinator requests (heartbeat, join, sync, commits, etc.) through `ConnectionGroup.COORDINATION`
- `tests/test_client.py`
  - Updates mocks/expectations for grouped connection keys
  - Adds integration tests validating blocking vs non-blocking behavior across groups
- `tests/test_consumer.py`
  - Adds test ensuring `commit()` is not blocked by long-poll fetches

### Implementation Approach
The core implementation change is to partition broker connections by “group” instead of having a single connection per `node_id`. A new `ConnectionGroup` class defines at least two categories: `DEFAULT` and `COORDINATION`. In `AIOKafkaClient`, the internal connection map `_conns` now stores connections under a composite key `(node_id, group)`, and `_get_conn()` takes a `group` parameter when creating or retrieving a connection. The `send()` and `ready()` methods also accept `group=...` and route requests to the appropriate connection entry.

On the consumer group side, `GroupCoordinator` is updated so coordinator-sensitive calls (e.g., `LeaveGroup`, heartbeats, offset fetch/commit, join/sync group flows) can explicitly use the coordination connection group. Tests demonstrate the original head-of-line blocking (a long fetch delays a “vanilla” request on the same connection) and that using a different group allows concurrency because the requests use separate connections. This is a targeted change that avoids redesigning the request pipeline while addressing the blocking behavior.

### Potential Impact
This change affects low-level connection management and any code that relies on the client’s connection cache. It primarily impacts consumer group operations (offset commit, heartbeat, group join/sync) by reducing latency under load. It also changes the internal `_conns` key structure, so tests and any internal code accessing `_conns` must align. Operationally, it may increase the number of open sockets per broker (one per group), which can slightly increase resource usage.

---

## PR 2: #237
PR Link: https://github.com/aio-libs/aiokafka/pull/237

### PR Summary
This PR extends producer send results to include Kafka message timestamp information. Previously, `RecordMetadata` returned by producer futures contained topic/partition/offset but did not expose the record timestamp, even though Kafka can provide it (for example when brokers assign `LOG_APPEND_TIME`). The PR modifies `RecordMetadata` to include `timestamp` and `timestamp_type`, and adjusts the producer’s batching and accumulator logic so per-message futures and per-batch futures resolve with richer metadata. It also improves internal “append” APIs to return metadata objects rather than raw sizes, enabling the code to preserve the original user-provided timestamp when the broker indicates no timestamp was assigned. Tests are updated/added to validate behavior, including a test that simulates broker responses with log append time.

### Technical Changes (files/components modified)
- `aiokafka/structs.py`
  - Extends `RecordMetadata` namedtuple to include `timestamp` and `timestamp_type`
- `aiokafka/producer/message_accumulator.py`
  - Message batch builder returns metadata objects (not just sizes)
  - Tracks `(future, metadata)` pairs per appended message
  - Adds distinct completion paths:
    - `done(base_offset, timestamp)`
    - `done_noack()` for `acks=0`
    - `failure(exception)` for error propagation
- `aiokafka/producer/producer.py`
  - On produce responses, passes `timestamp` (when available) into batch completion
  - Uses `done_noack()` for required_acks=0
  - Changes `send_batch()` to return the shielded future from the accumulator
- `aiokafka/record/legacy_records.py`
  - `append()` returns a `LegacyRecordMetadata` object (offset/crc/size/timestamp) or `None`
- `aiokafka/util.py`
  - Simplifies `create_future()` to directly return `loop.create_future()`
- Tests updated:
  - `tests/record/test_legacy.py`
  - `tests/test_message_accumulator.py`
  - `tests/test_producer.py` (adds test for log append time)

### Implementation Approach
The PR implements timestamp support by expanding the metadata model and ensuring timestamp data survives from “message appended to batch” through “broker acknowledged produce request.” First, `RecordMetadata` is extended to include `timestamp` and `timestamp_type`. Next, the message accumulator and builders stop returning plain numeric sizes from `append()`; instead they return metadata objects (or `None` if full/closed). That allows the accumulator to remember details like relative offsets and the timestamp originally attached to the message.

When a produce response is received, the producer parses response tuples. For older API versions without timestamps, it sets a sentinel (e.g., `timestamp = -1`) to indicate “use user timestamp.” For newer response versions, it extracts the returned timestamp. The batch completion method (`done`) computes final offsets as `base_offset + relative_offset` and sets the message futures to `RecordMetadata` values. It also determines `timestamp_type` and chooses the right timestamp value when the broker returns `-1`. For `acks=0`, the new `done_noack()` resolves futures to `None` quickly, and `failure()` centralizes exception propagation.

### Potential Impact (50–100 words)
This PR impacts producer-facing APIs because `RecordMetadata` gains new fields; any downstream code constructing or unpacking it may need updates. Internally, it changes how append operations report success (metadata vs size), affecting message accumulator logic and tests. Functionally, users can now observe broker timestamps and distinguish create-time vs log-append-time behaviors, which may influence auditing, ordering assumptions, and metrics derived from produce results.
