# 06 · Security

Transport security covers the **client-facing business port** (business + `ADMIN_*` traffic
share it) with two capabilities, **both default off** — with defaults the behavior is byte-for-byte the plaintext baseline:

1. **TLS / mTLS** — encryption and mutual certificate auth;
2. **Business token auth** — HELLO carries `auth_token`, validated at handshake.

## 1. Server TLS

```properties
openlatch.server.tls.enabled=true
openlatch.server.tls.cert=/etc/openlatch/server-cert.pem
openlatch.server.tls.key=/etc/openlatch/server-key.pem
# optional mTLS:
openlatch.server.tls.require-client-cert=true
openlatch.server.tls.trust-store=/etc/openlatch/ca.pem
```

Behavior:

- plaintext connections are **rejected** once enabled (non-TLS bytes cut at the codec;
  handshake timeout 5s so half-open connections cannot squat resources);
- chain-validation failure / missing client cert under mTLS → disconnect with **zero session
  side effects** (no session id, no registry entry, no replicated entry);
- unreadable PEMs, unpaired cert/key, mTLS without a trust store → fail fast at startup, never half-started;
- **certificate rotation needs a restart** (no hot reload) — roll node-by-node per
  [05 planned restarts](05-cluster-deployment.md).

Throwaway test material via openssl (demo on one machine):

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout ca-key.pem -out ca.pem -subj "/CN=OpenLatch Test CA"
openssl req -newkey rsa:2048 -nodes -keyout server-key.pem -out server.csr -subj "/CN=localhost"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out server-cert.pem -days 365 -extfile <(echo "subjectAltName=DNS:localhost,IP:127.0.0.1")
```

In production use trusted CAs and real hostnames/SANs.

## 2. Business token auth

```properties
openlatch.server.auth.enabled=true
openlatch.server.auth.tokens=tok-A-2026,tok-B-rotate   # comma-separated; several may coexist
```

| Switch state | HELLO `auth_token` behavior |
|---|---|
| `enabled=false` (default) | compatibility guard: **a non-empty token is rejected** (pre-auth deployments do not accept tokens) |
| `enabled=true` | missing / empty / mismatch / server-unconfigured → uniform `INVALID_REQUEST` + disconnect |

- the verdict happens **before session allocation, registration and replication** — a rejected
  handshake leaves zero state;
- comparisons are **constant-time** and failures are indistinguishable (no enumeration oracle);
- authed once at handshake; the session lives with the connection (no per-request checks);
- **independent of the admin token**: `openlatch.server.admin.token` is the per-message
  read-only observation credential — the two token systems never substitute for each other.

## 3. Token rotation (three steps, no downtime)

1. **Server adds new**: append the new token to `auth.tokens` (old+new active) → rolling-restart servers;
2. **Clients switch**: update `auth-token` on every client/console → rolling-restart them;
3. **Server drops old**: remove the old token → rolling-restart servers.

> Order matters: dropping the old token server-first cuts every not-yet-rotated client
> (including older clients without valid tokens).

## 4. Consumer-side configuration

SDK, starter and console expose the same semantic surface (`tls-enabled`,
`tls-trust-store`, `tls-client-cert`/`tls-client-key` (paired), `auth-token`); once set,
TLS + token apply to first connect, reconnects and seed-discovery probes — all connection
construction points. Pages: [03](03-client-sdk.md), [04](04-spring-boot-starter.md), [07](07-admin-console.md).

## 5. Boundaries (honest scope)

| Channel | Covered? |
|---|---|
| client business port (incl. ADMIN observation traffic) | ✅ TLS / mTLS / token |
| Raft inter-node replication | ❌ plaintext — intranet / firewall / service mesh |
| metrics 9412, console web 9413 | ❌ plaintext — intranet / reverse proxy |

No end-to-end TLS claim. Console protection = admin token + network isolation; to expose
the console put a TLS-terminating reverse proxy in front. The metrics port is unauthenticated —
the scrape surface *is* the runtime state surface; segment it accordingly.
