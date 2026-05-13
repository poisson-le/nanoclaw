# Planned items (local fork)

These are designed or identified but not yet implemented. They are specific to this installation and are not queued for upstream submission in their current state.

---

## 1. Telegram: add_reaction on inbound messages

**File:** `src/channels/telegram.ts`

`add_reaction` currently only works on outbound messages. When the agent needs to react to an inbound user message, the Telegram adapter has no path to call `setMessageReaction` on the original message.

**Planned:** Add inbound reaction support to the Telegram adapter via `setMessageReaction`.

**Blocker:** Design not finalised — needs a decision on trigger surface (MCP tool vs. automatic behaviour).

---

## 2. MCP output sandbox (strip-and-alert)

A structural pipeline to strip directive content from MCP-returned data before it reaches the agent's reasoning layer. When stripping occurs, alert the user with the source and the stripped text.

**Planned:** Implement as a NanoClaw-level filter applied to all MCP tool results before they are passed to the provider.

**Blocker:** Needs design — scope, filter rules, and alert format not yet decided.

---

## 3. chattr +a on agent log directories

Append-only enforcement (`chattr +a`) for compliance log directories to prevent tampering or deletion by the agent.

**Planned:** Apply at container startup on the relevant log paths.

**Constraint:** Only effective if agent containers run as non-root. Current container setup needs to be verified against this requirement before implementing.

**Blocker:** Needs design — container privilege model review required first.

---

## 4. Central log store with per-agent subdirectories

Architecture for a compliance monitoring agent to read all agent logs across groups. Current logs are per-session and per-agent-group with no central aggregation point.

**Planned:** Define a central log directory structure with per-agent subdirs, written to by each container and readable by a designated monitoring agent.

**Blocker:** Needs design — write path, rotation, access control, and monitoring agent wiring not yet decided.

---

## 5. Vault proxy HTTPS CONNECT fix

**File:** `src/` (vault proxy, port 10255)

The NanoClaw vault proxy does not support HTTPS CONNECT tunnelling used by axios-based MCP clients. Any MCP that uses axios for HTTP will return errors when routed through the proxy.

**Current workaround:** Add `NO_PROXY=<api-domain>` and `no_proxy=<api-domain>` to the affected MCP's `env` block in `container.json`. Confirmed affected: `tavily-mcp`.

**Planned:** Fix the proxy to handle CONNECT properly so axios-based MCPs work without per-MCP workarounds.

**Blocker:** Needs design.
