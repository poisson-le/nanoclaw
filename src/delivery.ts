/**
 * Outbound message delivery.
 * Polls session outbound DBs for undelivered messages, delivers through channel adapters.
 *
 * Two-DB architecture:
 *   - Reads messages_out from outbound.db (container-owned, opened read-only)
 *   - Tracks delivery in inbound.db's `delivered` table (host-owned)
 *   - Never writes to outbound.db — preserves single-writer-per-file invariant
 */
import fs from 'fs';
import path from 'path';

import type Database from 'better-sqlite3';

import { getRunningSessions, getActiveSessions, createPendingQuestion } from './db/sessions.js';
import { getAgentGroup } from './db/agent-groups.js';
import { getDb, hasTable } from './db/connection.js';
import { getMessagingGroupByPlatform } from './db/messaging-groups.js';
import {
  getDueOutboundMessages,
  getDeliveredIds,
  markDelivered,
  markDeliveryFailed,
  migrateDeliveredTable,
} from './db/session-db.js';
import { log } from './log.js';
import { normalizeOptions } from './channels/ask-question.js';
import { clearOutbox, openInboundDb, openOutboundDb, readOutboxFiles } from './session-manager.js';
import { pauseTypingRefreshAfterDelivery, setTypingAdapter } from './modules/typing/index.js';
import type { OutboundFile } from './channels/adapter.js';
import type { Session } from './types.js';

const ACTIVE_POLL_MS = 1000;
const SWEEP_POLL_MS = 60_000;
const MAX_DELIVERY_ATTEMPTS = 3;

// Compliance is a sink-style agent: it should not spawn a container for every
// cc'd inbound, and its conversational replies must never loop back to senders.
// Two guards in deliverMessage below: (1) outbound filter — drop Compliance's
// outbound unless it begins with "Compliance alert"; (2) ingest archive —
// when an agent targets Compliance, write the envelope to a host-managed log
// directory (Compliance reads it via a RO mount) and skip the agent route.
// Hardcoded for now — if other sink agents emerge, promote to a column on
// `agent_groups`.
const COMPLIANCE_AGENT_GROUP_ID = 'ag-1779121017094-2i35ns';
const COMPLIANCE_LOGS_DIR = '/home/aaron/nanoclaw-v2/data/compliance-logs';

// BountyHunter → TianYi normalisation: structural extraction host-side after
// BH's seed-level format enforcement failed repeatedly even with hardening.
// See src/modules/normalisers/bountyhunter.ts for the rationale.
const BOUNTYHUNTER_AGENT_GROUP_ID = 'ag-1779182034060-e7cwla';
const TIANYI_AGENT_GROUP_ID = 'ag-1779158848014-nyfezd';

/**
 * Strip dispatch-receipt seq numbers from user-facing chat text.
 *
 * The `send_message` MCP tool returns the per-session outbound seq id on
 * success — e.g. `"Message sent to literature_scout (id: 161)"`. Agents
 * frequently surface this in their next user-facing reply ("Done — sent as
 * message 161", "Brief forwarded (message 157)"). It's useful diagnostic
 * value internally (a real seq is evidence of a real tool call) but looks
 * unsophisticated to end users (especially collaborators who don't know
 * what the number refers to). We strip it from the channel-bound copy
 * only; the audit detector saw the original above, and the Compliance
 * archive captures the raw form before this strip runs.
 *
 * Only matches the specific dispatch-receipt patterns. Standalone
 * references like "message #37 above" (intentional cross-reference in
 * prose) are left intact.
 */
function stripDispatchSeqReceipts(text: string): string {
  return text
    .replace(/\s*\(id:\s*\d+\)/g, '') // "(id: 161)"
    .replace(/\s*\(message\s+\d+\)/g, '') // "(message 157)"
    .replace(/\s+as\s+message\s+#?\d+(?=[\s.,;:!?—–\-]|$)/gi, '') // "as message 161"
    .replace(/\s+\(message id:?\s*\d+\)/gi, '') // "(message id: 161)"
    .replace(/[  ]{2,}/g, ' ') // collapse residual double-spaces
    .replace(/\s+([.,;:!?])/g, '$1'); // clean up space-before-punctuation
}

interface ComplianceEnvelope {
  from: string;
  to: string;
  timestamp: string;
  content: unknown;
}

/**
 * Detect a well-formed cc envelope inside an agent-to-agent message.
 * Returns the parsed envelope iff the message's `text` field parses as JSON
 * with all four required string fields (from, to, timestamp) plus content.
 * Anything else (raw text, missing fields, malformed JSON) returns null,
 * signalling the caller to fall through to normal routing — used so direct
 * admin/bootstrap messages can still reach Compliance's container.
 */
function parseComplianceEnvelope(content: unknown): ComplianceEnvelope | null {
  const text = typeof (content as { text?: unknown })?.text === 'string' ? (content as { text: string }).text : '';
  if (!text) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== 'object') return null;
  const p = parsed as Record<string, unknown>;
  if (typeof p.from !== 'string' || !p.from) return null;
  if (typeof p.to !== 'string' || !p.to) return null;
  if (typeof p.timestamp !== 'string' || !p.timestamp) return null;
  if (!('content' in p)) return null;
  return { from: p.from, to: p.to, timestamp: p.timestamp, content: p.content };
}

function archiveComplianceEnvelope(envelope: ComplianceEnvelope): void {
  const day = new Date().toISOString().slice(0, 10);
  const dir = path.join(COMPLIANCE_LOGS_DIR, envelope.from);
  fs.mkdirSync(dir, { recursive: true });
  fs.appendFileSync(path.join(dir, `${day}.jsonl`), JSON.stringify(envelope) + '\n');
}

/**
 * Host-side auto-cc for Compliance. Any agent that has a `compliance`
 * destination wired gets its outbound automatically archived to the
 * Compliance log directory — no seed-level cc directive required, no
 * agent-side participation. Replaces the fragile seed-enforced cc that
 * relied on the model remembering to emit a `<message to="compliance">`
 * block on every reply (Claude Code preset frequently overrides this).
 *
 * Opt-in by destination wiring: if an agent has no `compliance` destination,
 * no auto-archive happens. This keeps the mechanism explicit and lets
 * specific agents (e.g. Compliance itself, or future opt-out cases) skip
 * archival cleanly.
 *
 * Best-effort: any error here is logged and swallowed; we never block
 * delivery on archive failure.
 */
function autoArchiveAgentOutbound(
  sourceAgentGroupId: string,
  msg: { channel_type: string | null; platform_id: string | null },
  content: { text?: unknown; operation?: unknown; emoji?: unknown; messageId?: unknown; files?: unknown },
): void {
  // Skip Compliance's own outbound — it doesn't audit itself.
  if (sourceAgentGroupId === COMPLIANCE_AGENT_GROUP_ID) return;
  // Skip messages going TO Compliance — the dedicated ingest path handles those.
  if (msg.channel_type === 'agent' && msg.platform_id === COMPLIANCE_AGENT_GROUP_ID) return;

  // Source agent must have a 'compliance' destination wired to opt in.
  // Direct DB read rather than the projection (which lives in inbound.db per
  // session and we're host-side here without that handle).
  const row = getDb()
    .prepare(
      "SELECT 1 FROM agent_destinations WHERE agent_group_id = ? AND local_name = 'compliance' AND target_id = ? LIMIT 1",
    )
    .get(sourceAgentGroupId, COMPLIANCE_AGENT_GROUP_ID);
  if (!row) return;

  const sourceAgent = getAgentGroup(sourceAgentGroupId);
  if (!sourceAgent) return;
  const fromName = sourceAgent.name.toLowerCase();

  // Forwards of Compliance alerts are themselves the alert content — don't
  // double-archive (the original alert already left a record on Compliance's
  // side via its own dispatch).
  const text = typeof content.text === 'string' ? (content.text as string) : '';
  if (text.startsWith('Compliance alert')) return;

  // Build a literal `content` value mirroring the agent-side cc convention.
  let contentValue: string;
  if (text) {
    contentValue = text;
  } else if (content.operation === 'reaction' && content.emoji && content.messageId) {
    contentValue = `[reaction: ${String(content.emoji)} on message #${String(content.messageId)}]`;
  } else if (Array.isArray(content.files) && content.files.length > 0) {
    contentValue = `[sent file: ${content.files.join(', ')}]`;
  } else {
    contentValue = JSON.stringify(content);
  }

  // Resolve target label. For agent-targeted messages use the target agent
  // name; for channel messages use the platform_id verbatim.
  let toLabel: string;
  if (msg.channel_type === 'agent' && msg.platform_id) {
    const targetAgent = getAgentGroup(msg.platform_id);
    toLabel = targetAgent ? targetAgent.name.toLowerCase() : msg.platform_id;
  } else {
    toLabel = msg.platform_id ?? '';
  }

  const envelope: ComplianceEnvelope = {
    from: fromName,
    to: toLabel,
    timestamp: new Date().toISOString(),
    content: contentValue,
  };
  archiveComplianceEnvelope(envelope);
}

/** Track delivery attempt counts. Resets on process restart (gives failed messages a fresh chance). */
const deliveryAttempts = new Map<string, number>();

/**
 * Sessions whose outbound queue is currently being drained.
 *
 * The active poll (1s, running sessions) and the sweep poll (60s, all
 * active sessions) both call deliverSessionMessages, and a running session
 * is in *both* result sets. Without this guard, the two timer chains can
 * race on the same outbound row: both read it as undelivered, both call
 * the channel adapter, both markDelivered (idempotent in the DB via
 * INSERT OR IGNORE — but the user has already seen the message twice).
 *
 * Skipping (vs. queueing) is correct: any message left over when the
 * second caller skips will be picked up on the next poll tick (~1s).
 */
const inflightDeliveries = new Set<string>();

export interface ChannelDeliveryAdapter {
  deliver(
    channelType: string,
    platformId: string,
    threadId: string | null,
    kind: string,
    content: string,
    files?: OutboundFile[],
  ): Promise<string | undefined>;
  setTyping?(channelType: string, platformId: string, threadId: string | null): Promise<void>;
}

let deliveryAdapter: ChannelDeliveryAdapter | null = null;
let activePolling = false;
let sweepPolling = false;

/**
 * Callbacks fired when the delivery adapter is first set (and again if it's
 * replaced). Lets modules that need the adapter at boot (e.g. approvals →
 * OneCLI handler) hook in without core calling into the module directly.
 *
 * Not a general-purpose registry — narrow lifecycle hook only.
 */
type AdapterReadyCallback = (adapter: ChannelDeliveryAdapter) => void | Promise<void>;
const adapterReadyCallbacks: AdapterReadyCallback[] = [];

/** Current delivery adapter or null if not yet set. Modules use this in live
 *  message-flow handlers where the adapter is guaranteed to be set. For
 *  boot-time setup (before the adapter is ready), use onDeliveryAdapterReady. */
export function getDeliveryAdapter(): ChannelDeliveryAdapter | null {
  return deliveryAdapter;
}

export function onDeliveryAdapterReady(cb: AdapterReadyCallback): void {
  adapterReadyCallbacks.push(cb);
  if (deliveryAdapter) {
    // Already set — fire immediately so late registrations still run.
    void Promise.resolve()
      .then(() => cb(deliveryAdapter as ChannelDeliveryAdapter))
      .catch((err) => log.error('onDeliveryAdapterReady callback threw', { err }));
  }
}

export function setDeliveryAdapter(adapter: ChannelDeliveryAdapter): void {
  deliveryAdapter = adapter;
  // Forward to the typing module so it can fire setTyping on its own
  // interval. Direct call, not a registry — typing is a default module.
  setTypingAdapter(adapter);
  for (const cb of adapterReadyCallbacks) {
    void Promise.resolve()
      .then(() => cb(adapter))
      .catch((err) => log.error('onDeliveryAdapterReady callback threw', { err }));
  }
}

/** Start the active container poll loop (~1s). */
export function startActiveDeliveryPoll(): void {
  if (activePolling) return;
  activePolling = true;
  pollActive();
}

/** Start the sweep poll loop (~60s). */
export function startSweepDeliveryPoll(): void {
  if (sweepPolling) return;
  sweepPolling = true;
  pollSweep();
}

async function pollActive(): Promise<void> {
  if (!activePolling) return;

  try {
    const sessions = getRunningSessions();
    for (const session of sessions) {
      await deliverSessionMessages(session);
    }
  } catch (err) {
    log.error('Active delivery poll error', { err });
  }

  setTimeout(pollActive, ACTIVE_POLL_MS);
}

async function pollSweep(): Promise<void> {
  if (!sweepPolling) return;

  try {
    const sessions = getActiveSessions();
    for (const session of sessions) {
      await deliverSessionMessages(session);
    }
  } catch (err) {
    log.error('Sweep delivery poll error', { err });
  }

  setTimeout(pollSweep, SWEEP_POLL_MS);
}

export async function deliverSessionMessages(session: Session): Promise<void> {
  // Reject re-entry from a concurrent poll on the same session — see the
  // comment on inflightDeliveries above.
  if (inflightDeliveries.has(session.id)) return;
  inflightDeliveries.add(session.id);

  try {
    await drainSession(session);
  } finally {
    inflightDeliveries.delete(session.id);
  }
}

async function drainSession(session: Session): Promise<void> {
  const agentGroup = getAgentGroup(session.agent_group_id);
  if (!agentGroup) return;

  let outDb: Database.Database;
  let inDb: Database.Database;
  try {
    outDb = openOutboundDb(agentGroup.id, session.id);
    inDb = openInboundDb(agentGroup.id, session.id);
  } catch {
    return; // DBs might not exist yet
  }

  try {
    // Read all due messages from outbound.db (read-only)
    const allDue = getDueOutboundMessages(outDb);
    if (allDue.length === 0) return;

    // Filter out already-delivered messages using inbound.db's delivered table
    const delivered = getDeliveredIds(inDb);
    const undelivered = allDue.filter((m) => !delivered.has(m.id));
    if (undelivered.length === 0) return;

    // Ensure platform_message_id column exists (migration for existing sessions)
    migrateDeliveredTable(inDb);

    for (const msg of undelivered) {
      try {
        const platformMsgId = await deliverMessage(msg, session, inDb);
        markDelivered(inDb, msg.id, platformMsgId ?? null);
        deliveryAttempts.delete(msg.id);

        // Pause the typing indicator after a real user-facing message
        // lands on the user's screen, so the client has time to visually
        // clear the indicator before the next heartbeat tick brings it
        // back. Skip the pause for internal traffic (system actions,
        // agent-to-agent routing) — the user doesn't see those and
        // shouldn't get a gap in their typing indicator for them.
        if (msg.kind !== 'system' && msg.channel_type !== 'agent') {
          pauseTypingRefreshAfterDelivery(session.id);
        }
      } catch (err) {
        const attempts = (deliveryAttempts.get(msg.id) ?? 0) + 1;
        deliveryAttempts.set(msg.id, attempts);
        if (attempts >= MAX_DELIVERY_ATTEMPTS) {
          log.error('Message delivery failed permanently, giving up', {
            messageId: msg.id,
            sessionId: session.id,
            attempts,
            err,
          });
          markDeliveryFailed(inDb, msg.id);
          deliveryAttempts.delete(msg.id);
        } else {
          log.warn('Message delivery failed, will retry', {
            messageId: msg.id,
            sessionId: session.id,
            attempt: attempts,
            maxAttempts: MAX_DELIVERY_ATTEMPTS,
            err,
          });
        }
      }
    }
  } finally {
    outDb.close();
    inDb.close();
  }
}

async function deliverMessage(
  msg: {
    id: string;
    kind: string;
    platform_id: string | null;
    channel_type: string | null;
    thread_id: string | null;
    content: string;
  },
  session: Session,
  inDb: Database.Database,
): Promise<string | undefined> {
  if (!deliveryAdapter) {
    log.warn('No delivery adapter configured, dropping message', { id: msg.id });
    return;
  }

  const content = JSON.parse(msg.content);

  // System actions — handle internally (schedule_task, cancel_task, etc.)
  if (msg.kind === 'system') {
    await handleSystemAction(content, session, inDb);
    return;
  }

  // Host-side Compliance auto-cc: archive every outbound from any agent that
  // has a `compliance` destination wired. Replaces fragile seed-based cc.
  // Best-effort — never blocks delivery.
  if (msg.kind === 'chat') {
    try {
      autoArchiveAgentOutbound(session.agent_group_id, msg, content);
    } catch (err) {
      log.error('Compliance auto-archive failed', { msgId: msg.id, err });
    }
  }

  // Generic dispatch-claim audit: catch any agent (not just TianYi) that
  // says it dispatched to a peer without actually invoking send_message.
  // Runs on user-facing chat outbounds (not agent-to-agent). When a claim
  // is detected without a matching actual dispatch in the last 60s, the
  // outbound text gets a visible warning appended so the user knows to
  // verify before waiting indefinitely. See src/modules/audits/dispatch-claim-detector.ts
  // for the rationale.
  if (msg.kind === 'chat' && msg.channel_type !== 'agent') {
    const outboundText =
      typeof (content as { text?: unknown })?.text === 'string' ? (content as { text: string }).text : '';
    if (outboundText.length >= 10) {
      try {
        const { auditDispatchClaims, buildAuditWarning } = await import('./modules/audits/dispatch-claim-detector.js');
        const suspected = auditDispatchClaims(session.agent_group_id, session.id, outboundText);
        if (suspected.length > 0) {
          const warning = buildAuditWarning(suspected);
          (content as { text: string }).text = outboundText + warning;
          msg.content = JSON.stringify(content);
          log.warn('Dispatch-claim audit flagged hallucinated dispatch', {
            msgId: msg.id,
            sourceAgentGroupId: session.agent_group_id,
            suspected: suspected.map((s) => ({
              dest: s.destination_name,
              evidence: s.evidence_phrase.substring(0, 80),
            })),
          });
        }
      } catch (err) {
        log.error('Dispatch-claim audit failed (non-fatal)', { msgId: msg.id, err });
      }
    }

    // Strip dispatch-receipt seq numbers from user-facing text. The
    // send_message MCP tool returns "Message sent to X (id: <seq>)" on
    // success, and agents tend to surface the seq in their user-facing
    // reply ("Done — sent as message 161"). Useful diagnostic but looks
    // unsophisticated to end users (especially collaborators who don't
    // know what the number refers to). The original text was already
    // seen by the audit detector above and is preserved in the
    // Compliance archive — only the user-facing copy is cleaned.
    const finalText =
      typeof (content as { text?: unknown })?.text === 'string' ? (content as { text: string }).text : '';
    if (finalText) {
      const cleaned = stripDispatchSeqReceipts(finalText);
      if (cleaned !== finalText) {
        (content as { text: string }).text = cleaned;
        msg.content = JSON.stringify(content);
      }
    }
  }

  // BH → TianYi normaliser: structural extraction host-side. Runs after the
  // Compliance auto-archive (so Compliance gets the raw text) and before the
  // agent route (so the annotation is in the message TianYi will read). Mutates
  // msg.content in-place to append the annotation block. Best-effort — any
  // failure here is logged and the original outbound flows through unchanged;
  // TianYi's seed has a fallback path for the missing-annotation case.
  if (
    msg.kind === 'chat' &&
    msg.channel_type === 'agent' &&
    session.agent_group_id === BOUNTYHUNTER_AGENT_GROUP_ID &&
    msg.platform_id === TIANYI_AGENT_GROUP_ID
  ) {
    const bhText = typeof (content as { text?: unknown })?.text === 'string' ? (content as { text: string }).text : '';
    const { shouldNormalise, normaliseBountyHunterOutput, persistExtract } =
      await import('./modules/normalisers/bountyhunter.js');
    if (shouldNormalise(bhText)) {
      try {
        log.info('BH normaliser starting', { msgId: msg.id, length: bhText.length });
        const extract = await normaliseBountyHunterOutput(bhText);
        if (extract) {
          const { extractPath, annotation } = persistExtract(session.id, msg.id, extract);
          // Mutate the in-memory content with the annotation appended, then
          // re-serialise into msg.content so downstream routing carries it.
          (content as { text: string }).text = bhText + annotation;
          msg.content = JSON.stringify(content);
          log.info('BH normaliser complete', {
            msgId: msg.id,
            extractPath,
            grade: extract.grade,
            modesAddressed: Object.values(extract.failure_modes).filter((m) => m.addressed).length,
          });
        }
      } catch (err) {
        log.error('BH normaliser failed (non-fatal)', { msgId: msg.id, err });
      }
    } else {
      log.debug('BH outbound skipped by normaliser heuristic', {
        msgId: msg.id,
        length: bhText.length,
      });
    }
  }

  // Agent-to-agent — route to target session via the agent-to-agent module.
  // Guarded by the channel_type check. If the module isn't installed the
  // `agent_destinations` table won't exist and `routeAgentMessage`'s permission
  // check will throw, which falls into the normal retry → mark-failed path.
  if (msg.channel_type === 'agent') {
    // Compliance outbound filter — see COMPLIANCE_AGENT_GROUP_ID block above.
    if (session.agent_group_id === COMPLIANCE_AGENT_GROUP_ID) {
      const text = typeof (content as { text?: unknown })?.text === 'string' ? (content as { text: string }).text : '';
      if (!text.startsWith('Compliance alert')) {
        log.info('Compliance outbound dropped (not an alert)', {
          msgId: msg.id,
          targetAgentGroupId: msg.platform_id,
          preview: text.substring(0, 60),
        });
        return;
      }
    }

    // Compliance ingest — when an agent targets Compliance with a well-formed
    // cc envelope, archive to the host-managed log directory and skip routing.
    // Messages that target Compliance but aren't envelopes (admin instructions,
    // bootstrap commands, direct queries) fall through to normal routing so
    // they actually reach Compliance's container.
    if (msg.platform_id === COMPLIANCE_AGENT_GROUP_ID) {
      const envelope = parseComplianceEnvelope(content);
      if (envelope) {
        try {
          archiveComplianceEnvelope(envelope);
          log.info('Compliance ingest archived', {
            msgId: msg.id,
            from: envelope.from,
          });
        } catch (err) {
          log.error('Compliance archive failed', { msgId: msg.id, err });
          // Swallow — losing one log line shouldn't block delivery.
        }
        return;
      }
      log.info('Compliance direct message routed (not an envelope)', {
        msgId: msg.id,
        from: session.agent_group_id,
      });
      // fall through to normal routing
    }

    if (!hasTable(getDb(), 'agent_destinations')) {
      throw new Error(`agent-to-agent module not installed — cannot route message ${msg.id}`);
    }
    const { routeAgentMessage } = await import('./modules/agent-to-agent/agent-route.js');
    await routeAgentMessage(msg, session);
    return;
  }

  // Permission check: the source agent must be allowed to deliver to this
  // channel destination. Two ways it passes:
  //
  //   1. The target is the session's own origin chat (session.messaging_group_id
  //      matches). An agent can always reply to the chat it was spawned from;
  //      requiring a destinations row for the obvious case is a footgun.
  //
  //   2. Otherwise, the agent must have an explicit agent_destinations row
  //      targeting that messaging group. createMessagingGroupAgent() inserts
  //      these automatically when wiring, so an operator wiring additional
  //      chats to the agent doesn't need a separate ACL step.
  //
  // Failures throw — unlike a silent `return`, an Error falls into the retry
  // path in deliverSessionMessages and eventually marks the message as failed
  // (instead of marking it delivered when nothing was actually delivered,
  // which was the pre-refactor bug).
  if (msg.channel_type && msg.platform_id) {
    const mg = getMessagingGroupByPlatform(msg.channel_type, msg.platform_id);
    if (!mg) {
      throw new Error(`unknown messaging group for ${msg.channel_type}/${msg.platform_id} (message ${msg.id})`);
    }
    const isOriginChat = session.messaging_group_id === mg.id;
    // Guarded: without the agent-to-agent module, `agent_destinations`
    // doesn't exist and we permit all non-origin channel sends (the
    // origin-chat case is always allowed regardless). Inlined SQL instead
    // of importing `hasDestination` so core doesn't depend on the module.
    if (!isOriginChat && hasTable(getDb(), 'agent_destinations')) {
      const row = getDb()
        .prepare(
          'SELECT 1 FROM agent_destinations WHERE agent_group_id = ? AND target_type = ? AND target_id = ? LIMIT 1',
        )
        .get(session.agent_group_id, 'channel', mg.id);
      if (!row) {
        throw new Error(
          `unauthorized channel destination: ${session.agent_group_id} cannot send to ${mg.channel_type}/${mg.platform_id}`,
        );
      }
    }
  }

  // Track pending questions for ask_user_question flow.
  // Guarded: without the interactive module, `pending_questions` doesn't
  // exist and we skip persistence — the card still delivers to the user,
  // but the response path has nowhere to land and will log unclaimed.
  if (content.type === 'ask_question' && content.questionId && hasTable(getDb(), 'pending_questions')) {
    const title = content.title as string | undefined;
    const rawOptions = content.options as unknown;
    if (!title || !Array.isArray(rawOptions)) {
      log.error('ask_question missing required title/options — not persisting', {
        questionId: content.questionId,
      });
    } else {
      const inserted = createPendingQuestion({
        question_id: content.questionId,
        session_id: session.id,
        message_out_id: msg.id,
        platform_id: msg.platform_id,
        channel_type: msg.channel_type,
        thread_id: msg.thread_id,
        title,
        options: normalizeOptions(rawOptions as never),
        created_at: new Date().toISOString(),
      });
      if (inserted) {
        log.info('Pending question created', { questionId: content.questionId, sessionId: session.id });
      }
    }
  }

  // Channel delivery
  if (!msg.channel_type || !msg.platform_id) {
    log.warn('Message missing routing fields', { id: msg.id });
    return;
  }

  // Read file attachments from outbox if the content declares files.
  // File I/O lives in session-manager.ts (symmetric with inbound
  // extractAttachmentFiles) — delivery just hands buffers to the adapter.
  const files =
    Array.isArray(content.files) && content.files.length > 0
      ? readOutboxFiles(session.agent_group_id, session.id, msg.id, content.files as string[])
      : undefined;

  const platformMsgId = await deliveryAdapter.deliver(
    msg.channel_type,
    msg.platform_id,
    msg.thread_id,
    msg.kind,
    msg.content,
    files,
  );
  log.info('Message delivered', {
    id: msg.id,
    channelType: msg.channel_type,
    platformId: msg.platform_id,
    platformMsgId,
    fileCount: files?.length,
  });

  clearOutbox(session.agent_group_id, session.id, msg.id);

  return platformMsgId;
}

/**
 * Delivery action registry.
 *
 * Modules register handlers for system-kind outbound message actions via
 * `registerDeliveryAction`. Core checks the registry first in
 * `handleSystemAction` and falls through to the inline switch when no
 * handler is registered. The switch will shrink as modules are extracted
 * (scheduling, approvals, agent-to-agent) and eventually only its default
 * branch remains.
 *
 * Default when no handler registered and the switch doesn't match: log
 * "Unknown system action" and return.
 */
export type DeliveryActionHandler = (
  content: Record<string, unknown>,
  session: Session,
  inDb: Database.Database,
) => Promise<void>;

const actionHandlers = new Map<string, DeliveryActionHandler>();

export function registerDeliveryAction(action: string, handler: DeliveryActionHandler): void {
  if (actionHandlers.has(action)) {
    log.warn('Delivery action handler overwritten', { action });
  }
  actionHandlers.set(action, handler);
}

/**
 * Handle system actions from the container agent.
 * These are written to messages_out because the container can't write to inbound.db.
 * The host applies them to inbound.db here.
 */
async function handleSystemAction(
  content: Record<string, unknown>,
  session: Session,
  inDb: Database.Database,
): Promise<void> {
  const action = content.action as string;
  log.info('System action from agent', { sessionId: session.id, action });

  const registered = actionHandlers.get(action);
  if (registered) {
    await registered(content, session, inDb);
    return;
  }

  log.warn('Unknown system action', { action });
}

export function stopDeliveryPolls(): void {
  activePolling = false;
  sweepPolling = false;
}
