---
name: deterministic-http-transport-cleanup-testing
description: "Reproduce HTTP client failures caused by concurrent test-server cleanup closing a shared process-wide transport."
when_to_use: "Use when parallel Go HTTP tests intermittently fail with transport lifecycle errors such as `http: CloseIdleConnections called` while another `httptest.Server` is closing."
---

# Test shared HTTP transport cleanup deterministically

- Verify the active Go toolchain's `httptest.Server.Close` implementation before assigning causality. It may call `http.DefaultTransport.CloseIdleConnections`, so separate `http.Client` values with nil transports still share lifecycle state.
- Replace `http.DefaultTransport` in one non-parallel top-level test with a close-sensitive `http.RoundTripper`. Have `RoundTrip` signal that delivery started, wait for `CloseIdleConnections`, and then return the recorded transport error. Restore the original transport with test cleanup before parallel tests resume.
- Use a separate `httptest.Server` solely to invoke the real cleanup path. Coordinate delivery and cleanup with channels and `sync.Once`; do not use sleeps to widen the race.
- Arrange the readiness signal so both implementations progress: the close-sensitive transport signals it when the buggy client uses the process default, while the target server handler signals it when an isolated client reaches the server. This lets the unchanged test fail with the observed error before the fix and complete without timing assumptions afterward.
- Assert the ownership invariant separately by constructing two clients and verifying both transports are non-nil and distinct.
- Isolate lifecycle state by cloning the standard `*http.Transport` for each client. If the process default can be a custom `RoundTripper`, use an independent standard-settings fallback rather than silently sharing the custom transport.
- Prove the regression red against the old constructor, then repeat the focused test, the affected package with a high `-count`, the focused package under `-race`, and the repository validation gate.

## Confirm the real bodyless-response window

When a valid 204 is reported as a transport failure, prefer a real HTTP/1 transport reproduction before using a synthetic close-sensitive transport:

- Inspect the pinned Go version's bodyless-response read loop. It can pool the connection before delivering the response to RoundTrip.
- Attach httptrace.ClientTrace.PutIdleConn to the request. Signal entry through a channel and hold the callback on another channel; this parks the real response between pooling and delivery.
- After the signal, close an unrelated httptest.Server. With the shared default transport, wait for RoundTrip to return before releasing the callback. Assert the bounded error category identifies CloseIdleConnections, not a context deadline. Join the callback before ending the test.
- Run a control using the fixture-owned server.Client transport: close the unrelated server at the same point, release the callback, and assert successful response delivery. Check that fixture transports are non-nil, distinct from the default, and distinct across servers.
- Keep the global-default control non-parallel. Bound stalled requests with context cancellation and release trace callbacks on failure. Do not use sleeps or manufacture the expected transport error.
- For a fixture-only defect, inject server.Client with the intended timeout and redirect policy. Do not change production authorization semantics or add retries without separate production evidence.
- Distinguish a demonstrated failure mechanism from attribution of an earlier uninstrumented incident. Report named operations, response status, error type, and allowlisted failure categories; omit raw URLs, headers, bodies, and arbitrary transport error text.
