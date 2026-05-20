/**
 * Dispatch-claim audit.
 *
 * Catches the "claimed action without actual action" failure: an agent emits
 * a user-facing message saying it dispatched to a peer (e.g. "Scout is on it",
 * "dispatched to Bounty Hunter") but never actually called the send_message
 * tool to route a real agent-to-agent message. The user waits indefinitely
 * for results that will never come.
 *
 * This audit runs on every user-facing chat outbound at delivery time:
 * 1. Look up the source agent's wired destinations from agent_destinations.
 * 2. Build name variations for each destination (literature_scout → Scout, etc.)
 * 3. Scan the outbound text against dispatch-claim phrasings × destination names.
 * 4. For each suspected claim, check the SAME session's outbound.db for an
 *    actual `channel_type='agent'` outbound to that destination's agent_group_id
 *    in the last DISPATCH_WINDOW_SECONDS. Same Claude turn => present.
 * 5. If claim ∩ no-actual-dispatch → append a short visible warning to the
 *    outbound + log alert.
 *
 * False positives are tolerable (a warning the user can ignore); false negatives
 * are not (the original failure mode of silent waiting). The patterns are
 * intentionally specific to minimise false positives.
 *
 * Encountered: TianYi 2026-05-20 00:26 — said "Scout is on it" but never
 * dispatched. Would have wasted hours if Aaron hadn't been actively watching.
 */
import path from 'path';

import Database from 'better-sqlite3';

import { DATA_DIR } from '../../config.js';
import { getDb } from '../../db/connection.js';
import { log } from '../../log.js';

const DISPATCH_WINDOW_SECONDS = 60;

interface DestinationRow {
  local_name: string;
  target_id: string;
  display_name: string | null;
}

interface SuspectedClaim {
  destination_name: string; // the local_name from agent_destinations
  target_agent_group_id: string; // the resolved agent_group_id of the destination
  evidence_phrase: string; // the substring of the outbound that matched
}

/**
 * Build name variations for a destination so the regex catches whatever form
 * the agent actually used in its prose (snake_case, "Title Case", common
 * abbreviations).
 */
function nameVariations(localName: string, displayName: string | null): string[] {
  const variations = new Set<string>();
  variations.add(localName);
  variations.add(localName.replace(/_/g, ' '));
  variations.add(localName.replace(/_/g, ''));

  // Title-case version: literature_scout → Literature Scout
  const titleCase = localName
    .split('_')
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
  variations.add(titleCase);

  // CamelCase: bounty_hunter → BountyHunter
  variations.add(
    localName
      .split('_')
      .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
      .join(''),
  );

  if (displayName) {
    variations.add(displayName);
    // Display name without spaces
    variations.add(displayName.replace(/\s+/g, ''));
  }

  // Common short forms for known agents — strict list, not a generic rule.
  const SHORT_FORMS: Record<string, string[]> = {
    literature_scout: ['Scout', 'scout'],
    bounty_hunter: ['Bounty Hunter', 'bounty hunter', 'BH'],
    diamond_cutter: ['Diamond Cutter', 'diamond cutter', 'DC'],
    quality_control: ['Quality Control', 'quality control', 'QC'],
    quant_agent: ['Quant', 'quant', 'Quant Agent'],
    qual_agent: ['Qual', 'qual', 'Qual Agent'],
  };
  for (const sf of SHORT_FORMS[localName] ?? []) variations.add(sf);

  return Array.from(variations).filter((v) => v.length >= 2);
}

/**
 * Action-verb patterns that indicate "I dispatched X" rather than just
 * "X exists" or "we should consider X". Order matters less than coverage;
 * keep each one tight to its claim.
 */
const DISPATCH_VERB_PATTERNS = [
  // <Agent> is on it / will handle / is working / is now [verb]ing
  String.raw`(\bis on it\b|\bwill handle\b|\bis working on\b|\bis now \w+ing\b|\bhas started\b|\bis searching\b|\bis drafting\b|\bis running\b|\bhas been (?:asked|dispatched|tasked|sent))`,
  // Dispatched/sent/forwarded to <Agent>
  String.raw`\b(?:dispatched|sent|forwarded|forwarding|dispatching|sending|tasked|asked)\b[^.\n]{0,80}?\bto\b`,
  // Brief is with <Agent> / task is with <Agent>
  String.raw`\b(?:brief|task|request|dispatch) is (?:with|now with|in front of)\b`,
  // Note: the future-tense pattern ("I'll send / I will dispatch / going to forward")
  // was removed (2026-05-20) — it produced false positives when agents requested
  // confirmation before dispatching ("Confirm and I'll dispatch to Scout"), which
  // is permission-asking, not a claim of completed action. This detector audits
  // *claimed completed actions*; future-tense intent without follow-through is a
  // different failure class (silent stall) and belongs to a separate detector.
];

/**
 * Combine destination-name variations with dispatch-verb patterns into a
 * regex that catches "<verb> ... <agent>" or "<agent> ... <verb>" within a
 * short character window.
 */
function buildClaimRegex(nameVars: string[]): RegExp {
  const namePat = nameVars.map((v) => v.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|');
  // verb-then-name OR name-then-verb (with the verb being any of our patterns)
  const verbPat = DISPATCH_VERB_PATTERNS.join('|');
  const pattern =
    String.raw`(?:\b(?:${namePat})\b[^.\n]{0,80}?(?:${verbPat}))` +
    `|` +
    String.raw`(?:(?:${verbPat})[^.\n]{0,80}?\b(?:${namePat})\b)`;
  return new RegExp(pattern, 'i');
}

/**
 * Open the per-session outbound.db read-only and count agent-to-agent
 * messages routed from this session to a specific target agent group within
 * the lookback window. Returns true if any such dispatch was found.
 *
 * Note: this opens a fresh handle each audit. Per audit overhead is small
 * (single SELECT on an indexed column), and the alternative (caching
 * connections) would tangle with the cross-mount safety guarantees.
 */
function dispatchHappenedRecently(
  sessionId: string,
  agentGroupId: string,
  targetAgentGroupId: string,
  windowSeconds: number,
): boolean {
  const dbPath = path.join(DATA_DIR, 'v2-sessions', agentGroupId, sessionId, 'outbound.db');
  let db: Database.Database | null = null;
  try {
    db = new Database(dbPath, { readonly: true, fileMustExist: true });
    const cutoff = new Date(Date.now() - windowSeconds * 1000).toISOString();
    const row = db
      .prepare(
        `SELECT 1 FROM messages_out
         WHERE channel_type = 'agent'
           AND platform_id = ?
           AND datetime(timestamp) >= datetime(?)
         LIMIT 1`,
      )
      .get(targetAgentGroupId, cutoff);
    return row !== undefined;
  } catch (err) {
    log.warn('dispatchHappenedRecently: outbound.db read failed', {
      sessionId,
      agentGroupId,
      err: err instanceof Error ? err.message : String(err),
    });
    return true; // fail-open: don't false-positive on a DB read error
  } finally {
    try {
      db?.close();
    } catch {
      // ignore
    }
  }
}

/**
 * Audit a single user-facing outbound for dispatch claims that don't match
 * actual agent-to-agent sends. Returns an array of suspected hallucinations.
 */
export function auditDispatchClaims(
  sourceAgentGroupId: string,
  sessionId: string,
  outboundText: string,
): SuspectedClaim[] {
  if (!outboundText || outboundText.length < 10) return [];

  const destinations = getDb()
    .prepare(
      `SELECT ad.local_name, ad.target_id, ag.name as display_name
       FROM agent_destinations ad
       LEFT JOIN agent_groups ag ON ag.id = ad.target_id
       WHERE ad.agent_group_id = ? AND ad.target_type = 'agent'`,
    )
    .all(sourceAgentGroupId) as DestinationRow[];

  if (destinations.length === 0) return [];

  const suspected: SuspectedClaim[] = [];

  for (const dest of destinations) {
    // Skip self-referential or audit-irrelevant destinations.
    if (dest.local_name === 'compliance' || dest.local_name === 'parent') continue;

    const vars = nameVariations(dest.local_name, dest.display_name);
    const regex = buildClaimRegex(vars);
    const match = regex.exec(outboundText);
    if (!match) continue;

    // We have a textual claim. Check for the actual dispatch.
    const dispatched = dispatchHappenedRecently(sessionId, sourceAgentGroupId, dest.target_id, DISPATCH_WINDOW_SECONDS);

    if (!dispatched) {
      suspected.push({
        destination_name: dest.local_name,
        target_agent_group_id: dest.target_id,
        evidence_phrase: match[0].substring(0, 160),
      });
    }
  }

  return suspected;
}

/**
 * Build a compact human-readable warning string for appending to the
 * user-facing message. Returns empty string if there are no suspected claims.
 */
export function buildAuditWarning(suspected: SuspectedClaim[]): string {
  if (suspected.length === 0) return '';
  const lines: string[] = [];
  lines.push('');
  lines.push('---');
  lines.push('⚠️ **Host audit:** the agent appears to have claimed a dispatch that did not actually happen.');
  for (const s of suspected) {
    lines.push(
      `- Claimed dispatch to **${s.destination_name}** — no matching agent-to-agent send found within the last ${DISPATCH_WINDOW_SECONDS}s.`,
    );
    lines.push(`  Evidence: *"${s.evidence_phrase.trim()}"*`);
  }
  lines.push('');
  lines.push('Verify with the agent before assuming the dispatch has occurred.');
  return lines.join('\n');
}
