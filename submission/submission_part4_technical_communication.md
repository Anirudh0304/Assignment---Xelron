# Part 4 — Technical Communication (Task 4.1)

## Scenario Response (WRITE IN YOUR OWN WORDS; 250–350 words)

**Why I chose this PR over the others**  
I chose PR #196 (“Added separate socket groups to client”) because its goal is concrete and easy to validate: reduce head‑of‑line blocking caused by Kafka’s synchronous request/response behavior on a single connection. Compared to more sweeping refactors or large feature additions, this PR is localized to connection management and group coordination paths, and it includes tests that demonstrate the problem and the improved behavior. That made it easier for me to reason about scope, expected outcomes, and what “done” looks like.

**What made it comprehensible to me**  
My background includes working with asynchronous Python (`asyncio`), network clients, and systems where a single shared resource can become a bottleneck (e.g., a single event loop task/lock or a single socket). The PR’s approach—introducing a small “connection group” abstraction and routing coordination requests through a separate group—maps directly to a familiar concurrency pattern: isolate latency‑sensitive control traffic from potentially blocking data-plane traffic.

**Challenges I anticipate implementing it**  
The main risk is correctness around connection lifecycle and caching: changing the `_conns` mapping key from `node_id` to `(node_id, group)` can introduce subtle bugs if any call sites still assume the old shape. Another challenge is ensuring the change doesn’t create connection leaks or excessive socket creation, especially in error and reconnect paths. Finally, timing-based concurrency tests can be flaky in CI if not designed carefully.

**How I would overcome those challenges**  
I would address these by (1) systematically updating all call sites via search and adding targeted unit tests for the new connection-id behavior, (2) validating connection close/retry behavior per group, and (3) keeping the test assertions resilient (e.g., verifying “not blocked” using generous thresholds and explicit task states) while still proving separation of connections.