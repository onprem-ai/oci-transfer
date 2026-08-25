# Repository Instructions

## Non-negotiable error transparency

Errors must never be swallowed, replaced with generic text, or reduced to an
exception class name. Operators must receive the complete actionable reason for
a failure through exceptions, durable job state, error history, and public API
consumers.

- When translating an exception, preserve its message and relevant context.
- When an HTTP service returns an error, preserve its status and bounded response detail.
- Never use `except ...: pass`, return success after failure, or catch merely to discard.
- Cleanup, rollback, retry-classification, and conversion catches must preserve the detail or re-raise.
- Predicate probes may return false only when failure is genuinely equivalent to a negative result.
- Sanitize before persistence or display: redact credentials, auth headers, license keys, and URLs while retaining non-secret detail.
- Test that actionable details survive every error boundary and secrets remain redacted.

## Non-negotiable API design

The public execution model is async-only. Any public network, filesystem,
database, subprocess, or other potentially blocking operation must be async and
must not block the event loop. Callbacks and providers are async-only. Keep
resource ownership explicit and provide deterministic async cleanup.
