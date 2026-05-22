/**
 * Core MCP tools: send_message, send_file, edit_message, add_reaction.
 *
 * All outbound tools resolve destinations via the local destination map
 * (see destinations.ts). Agents reference destinations by name; the map
 * translates name → routing tuple. Permission enforcement happens on
 * the host side in delivery.ts via the agent_destinations table.
 */
import fs from 'fs';
import path from 'path';

import { findByName, getAllDestinations } from '../destinations.js';
import { getMessageIdBySeq, getRoutingBySeq, writeMessageOut } from '../db/messages-out.js';
import { getSessionRouting } from '../db/session-routing.js';
import { registerTools } from './server.js';
import type { McpToolDefinition } from './types.js';

function log(msg: string): void {
  console.error(`[mcp-tools] ${msg}`);
}

function generateId(): string {
  return `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function ok(text: string) {
  return { content: [{ type: 'text' as const, text }] };
}

function err(text: string) {
  return { content: [{ type: 'text' as const, text: `Error: ${text}` }], isError: true };
}

function destinationList(): string {
  const all = getAllDestinations();
  if (all.length === 0) return '(none)';
  return all.map((d) => d.name).join(', ');
}

/**
 * Resolve a destination name to routing fields.
 *
 * If `to` is omitted, use the session's default reply routing (channel +
 * thread the conversation is in) — the agent replies in place.
 *
 * If `to` is specified, look up the named destination. If it resolves to
 * the same channel the session is bound to, the session's thread_id is
 * preserved so replies land in the correct thread. Otherwise thread_id
 * is null (a cross-destination send starts a new conversation).
 */
function resolveRouting(
  to: string | undefined,
):
  | { channel_type: string; platform_id: string; thread_id: string | null; resolvedName: string }
  | { error: string } {
  if (!to) {
    // Default: reply to whatever thread/channel this session is bound to.
    const session = getSessionRouting();
    if (session.channel_type && session.platform_id) {
      return {
        channel_type: session.channel_type,
        platform_id: session.platform_id,
        thread_id: session.thread_id,
        resolvedName: '(current conversation)',
      };
    }
    // No session routing (e.g., agent-shared or internal-only agent) —
    // fall back to the legacy single-destination shortcut.
    const all = getAllDestinations();
    if (all.length === 0) return { error: 'No destinations configured.' };
    if (all.length > 1) {
      return {
        error: `You have multiple destinations — specify "to". Options: ${all.map((d) => d.name).join(', ')}`,
      };
    }
    to = all[0].name;
  }
  const dest = findByName(to);
  if (!dest) return { error: `Unknown destination "${to}". Known: ${destinationList()}` };
  if (dest.type === 'channel') {
    // If the destination is the same channel the session is bound to,
    // preserve the thread_id so replies land in the correct thread.
    const session = getSessionRouting();
    const threadId =
      session.channel_type === dest.channelType && session.platform_id === dest.platformId
        ? session.thread_id
        : null;
    return {
      channel_type: dest.channelType!,
      platform_id: dest.platformId!,
      thread_id: threadId,
      resolvedName: to,
    };
  }
  return { channel_type: 'agent', platform_id: dest.agentGroupId!, thread_id: null, resolvedName: to };
}

export const sendMessage: McpToolDefinition = {
  tool: {
    name: 'send_message',
    description:
      'Send a message to a named destination. If you have only one destination, you can omit `to`.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        to: { type: 'string', description: 'Destination name (e.g., "family", "worker-1"). Optional if you have only one destination.' },
        text: { type: 'string', description: 'Message content' },
      },
      required: ['text'],
    },
  },
  async handler(args) {
    const text = args.text as string;
    if (!text) return err('text is required');

    const routing = resolveRouting(args.to as string | undefined);
    if ('error' in routing) return err(routing.error);

    const id = generateId();
    const seq = writeMessageOut({
      id,
      kind: 'chat',
      platform_id: routing.platform_id,
      channel_type: routing.channel_type,
      thread_id: routing.thread_id,
      content: JSON.stringify({ text }),
    });

    log(`send_message: #${seq} → ${routing.resolvedName}`);
    return ok(`Message sent to ${routing.resolvedName} (id: ${seq})`);
  },
};

/**
 * Shared file-send handler used by both send_file and reply_with_files.
 *
 * Routing is the only thing that differs between the two tools — the file
 * staging + outbound write are identical. Centralising avoids drift if the
 * outbox layout or content schema changes.
 */
function performFileSend(args: {
  routing: { channel_type: string; platform_id: string; thread_id: string | null; resolvedName: string };
  filePath: string;
  filename?: string;
  text?: string;
  toolName: string;
}): ReturnType<typeof ok> | ReturnType<typeof err> {
  const resolvedPath = path.isAbsolute(args.filePath)
    ? args.filePath
    : path.resolve('/workspace/agent', args.filePath);
  if (!fs.existsSync(resolvedPath)) return err(`File not found: ${args.filePath}`);

  const id = generateId();
  const filename = args.filename || path.basename(resolvedPath);

  const outboxDir = path.join('/workspace/outbox', id);
  fs.mkdirSync(outboxDir, { recursive: true });
  fs.copyFileSync(resolvedPath, path.join(outboxDir, filename));

  writeMessageOut({
    id,
    kind: 'chat',
    platform_id: args.routing.platform_id,
    channel_type: args.routing.channel_type,
    thread_id: args.routing.thread_id,
    content: JSON.stringify({ text: args.text || '', files: [filename] }),
  });

  log(`${args.toolName}: ${id} → ${args.routing.resolvedName} (${filename})`);
  return ok(`File sent to ${args.routing.resolvedName} (id: ${id}, filename: ${filename})`);
}

export const sendFile: McpToolDefinition = {
  tool: {
    name: 'send_file',
    description:
      'Send a file to a named destination — used for forwarding files to a DIFFERENT conversation than the current one (e.g. cross-channel forward). To send a file as a reply to the current conversation, prefer `reply_with_files` instead — it routes automatically and never requires guessing a destination name.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        to: {
          type: 'string',
          description:
            'Destination name from your destinations list. Required when sending to a different conversation than the one you are currently in. If omitted, the file is sent to the current conversation (equivalent to reply_with_files).',
        },
        path: { type: 'string', description: 'File path (relative to /workspace/agent/ or absolute)' },
        text: { type: 'string', description: 'Optional accompanying message' },
        filename: { type: 'string', description: 'Display name (default: basename of path)' },
      },
      required: ['path'],
    },
  },
  async handler(args) {
    const filePath = args.path as string;
    if (!filePath) return err('path is required');

    const routing = resolveRouting(args.to as string | undefined);
    if ('error' in routing) return err(routing.error);

    return performFileSend({
      routing,
      filePath,
      filename: args.filename as string | undefined,
      text: args.text as string | undefined,
      toolName: 'send_file',
    });
  },
};

/**
 * reply_with_files — strict-reply file send.
 *
 * Use case: agent received a request via channel X, wants to reply with a
 * file attachment. Existing `send_file` supports this via "omit `to`", but
 * the tool description doesn't surface that path, so agents (incl. TianYi
 * on 2026-05-22) read `send_file` as "must specify destination" and pick a
 * named one — often the wrong one when the agent is wired to multiple
 * channels (DM + group). reply_with_files makes the intent unambiguous and
 * has no `to` parameter at all, so there's no destination to guess.
 *
 * Behaviour: routes via session_routing (the channel + platform + thread
 * the current message arrived on). Errors loudly if no session context
 * exists — the agent should use `send_file` instead in that case.
 */
export const replyWithFiles: McpToolDefinition = {
  tool: {
    name: 'reply_with_files',
    description:
      'Reply to the current conversation with a file attachment. Use this whenever you want to send a file back to whoever sent you the current request — same routing as a normal text reply, just with a file attached. Does NOT take a destination name; the file is delivered to the same channel the current message arrived on. If you need to send a file to a DIFFERENT conversation (cross-channel forward), use `send_file` instead.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        path: { type: 'string', description: 'File path (relative to /workspace/agent/ or absolute)' },
        text: { type: 'string', description: 'Optional accompanying message / caption' },
        filename: { type: 'string', description: 'Display name (default: basename of path)' },
      },
      required: ['path'],
    },
  },
  async handler(args) {
    const filePath = args.path as string;
    if (!filePath) return err('path is required');

    const session = getSessionRouting();
    if (!session.channel_type || !session.platform_id) {
      return err(
        'No active conversation context — reply_with_files needs an incoming message to reply to. ' +
          'If you meant to send to a specific destination, use send_file with `to` instead.',
      );
    }

    return performFileSend({
      routing: {
        channel_type: session.channel_type,
        platform_id: session.platform_id,
        thread_id: session.thread_id,
        resolvedName: '(current conversation)',
      },
      filePath,
      filename: args.filename as string | undefined,
      text: args.text as string | undefined,
      toolName: 'reply_with_files',
    });
  },
};

export const editMessage: McpToolDefinition = {
  tool: {
    name: 'edit_message',
    description: 'Edit a previously sent message. Targets the same destination the original message was sent to.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        messageId: { type: 'integer', description: 'Message ID (the numeric id shown in messages)' },
        text: { type: 'string', description: 'New message content' },
      },
      required: ['messageId', 'text'],
    },
  },
  async handler(args) {
    const seq = Number(args.messageId);
    const text = args.text as string;
    if (!seq || !text) return err('messageId and text are required');

    const platformId = getMessageIdBySeq(seq);
    if (!platformId) return err(`Message #${seq} not found`);

    const routing = getRoutingBySeq(seq);
    if (!routing || !routing.channel_type || !routing.platform_id) {
      return err(`Cannot determine destination for message #${seq}`);
    }

    const id = generateId();
    writeMessageOut({
      id,
      kind: 'chat',
      platform_id: routing.platform_id,
      channel_type: routing.channel_type,
      thread_id: routing.thread_id,
      content: JSON.stringify({ operation: 'edit', messageId: platformId, text }),
    });

    log(`edit_message: #${seq} → ${platformId}`);
    return ok(`Message edit queued for #${seq}`);
  },
};

export const addReaction: McpToolDefinition = {
  tool: {
    name: 'add_reaction',
    description: 'Add an emoji reaction to a message.',
    inputSchema: {
      type: 'object' as const,
      properties: {
        messageId: { type: 'integer', description: 'Message ID (the numeric id shown in messages)' },
        emoji: { type: 'string', description: 'Emoji name (e.g., thumbs_up, heart, check)' },
      },
      required: ['messageId', 'emoji'],
    },
  },
  async handler(args) {
    const seq = Number(args.messageId);
    const emoji = args.emoji as string;
    if (!seq || !emoji) return err('messageId and emoji are required');

    const platformId = getMessageIdBySeq(seq);
    if (!platformId) return err(`Message #${seq} not found`);

    const routing = getRoutingBySeq(seq);
    if (!routing || !routing.channel_type || !routing.platform_id) {
      return err(`Cannot determine destination for message #${seq}`);
    }

    const id = generateId();
    writeMessageOut({
      id,
      kind: 'chat',
      platform_id: routing.platform_id,
      channel_type: routing.channel_type,
      thread_id: routing.thread_id,
      content: JSON.stringify({ operation: 'reaction', messageId: platformId, emoji }),
    });

    log(`add_reaction: #${seq} → ${emoji} on ${platformId}`);
    return ok(`Reaction queued for #${seq}`);
  },
};

registerTools([sendMessage, sendFile, replyWithFiles, editMessage, addReaction]);
