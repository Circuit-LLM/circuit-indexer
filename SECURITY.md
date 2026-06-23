# Security

circuit-indexer is a write-only data pipeline that parses untrusted on-chain input. Security matters.

---

## Supported versions

| Version | Supported |
|---|---|
| 0.7.x (current) | ✅ |

---

## Scope

circuit-indexer is a write-only data pipeline process. Its security surface is:

- **Geyser event input** — reads JSON lines from a file, stdin, or a gRPC stream; all input is treated as untrusted and schema-validated before writing
- **Redis writes** — XREAD/SETEX only; no commands that modify Redis configuration or expose data externally
- **Postgres writes** — parameterised queries throughout; no raw string interpolation in SQL
- **No inbound network listener** — the indexer opens no HTTP or TCP ports
- **No authentication handling** — credentials (Redis URL, Postgres URL, gRPC token) are read from environment variables, never logged

---

## Reporting Vulnerabilities

Please do not open a public GitHub issue for security vulnerabilities.

Email: **security@circuitllm.xyz**

Include:
- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof of concept
- The version of circuit-indexer and Node.js you are using

We will acknowledge receipt within 48 hours and aim to issue a patch within 7 days for confirmed vulnerabilities.

---

## Responsible Disclosure

We ask that you give us reasonable time to address the issue before public disclosure. We will credit researchers who report valid vulnerabilities.
