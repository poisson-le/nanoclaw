/**
 * BountyHunter output normaliser.
 *
 * Fires on every BH → TianYi outbound that looks like a substantive analysis
 * (not a status message or supplementary literature request). Invokes Claude
 * Haiku via the local `claude` CLI in --print mode to extract a structured
 * JSON object from BH's free-form prose: grade mapping (any rubric → one of
 * Strong/Conditional/Weak/Do not pursue), failure-mode coverage (7 named
 * modes), gap type, paper-citation markers, named references for downstream
 * verification, research questions and hypotheses.
 *
 * Persists the extract to `data/normaliser-extracts/<bh-session>/<msg>.json`
 * and appends a compact `[system-annotation: bh-extract-ready] ... [/...]`
 * block to the outbound content so TianYi (downstream) reads the path and
 * summary inline in her inbound without needing to query anything.
 *
 * Best-effort: any failure is logged and the original outbound flows through
 * unchanged. TianYi's seed has a fallback path for the missing-extract case.
 *
 * Rationale: BH's seed enforcement of Strong/Conditional/Weak grade syntax,
 * named failure-mode headers, and [Pn] citation form has failed repeatedly
 * even after hardening. The Claude Code academic-writing prior is too strong
 * to override at the seed level for sophisticated analytical content. This
 * normaliser accepts BH's native voice and translates structure host-side —
 * the same architectural move as the Compliance black-hole filter (host-side
 * enforcement, not seed-side).
 */
import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';

import { DATA_DIR } from '../../config.js';
import { log } from '../../log.js';

const HAIKU_MODEL = 'haiku';
const MIN_CONTENT_LENGTH = 2500;
const NORMALISER_TIMEOUT_MS = 180_000;

const NORMALISER_SYSTEM_PROMPT = `You are a structural extractor for academic gap-analysis reports. You do NOT evaluate or critique the analysis. You ONLY read a free-form gap analysis written by another AI agent (BountyHunter) and return a JSON object with EXACTLY the fields and value-types described below.

OUTPUT FORMAT — return raw JSON only, no markdown fences, no commentary, no prose. The output must be parseable by JSON.parse() directly.

REQUIRED FIELDS (all required, do not omit any):

{
  "grade": <one of EXACTLY these four strings: "Strong" | "Conditional" | "Weak" | "Do not pursue">,
  "grade_rationale_one_sentence": <string, one sentence drawn from the analysis explaining the grade>,
  "grade_inferred": <boolean — true if you had to infer the grade from prose because no explicit grade was given; false if the input explicitly stated a grade>,
  "grade_original_form": <string — the EXACT text from the input conveying the grade, e.g. "Theoretical opportunity: B+, Venue opportunity: A−" — verbatim, not paraphrased; <= 200 chars>,
  "gap_type": <one of EXACTLY these five strings: "Theoretical" | "Empirical" | "Methodological" | "Application" | "Multiple">,
  "failure_modes": {
    "scope-too-narrow":          {"addressed": <bool>, "verdict": <one of "passes" | "flagged" | "fixed">, "note": <string <= 200 chars>},
    "scope-too-broad":           {"addressed": <bool>, "verdict": ..., "note": ...},
    "theoretical-insufficiency": {"addressed": <bool>, "verdict": ..., "note": ...},
    "application-ceiling":       {"addressed": <bool>, "verdict": ..., "note": ...},
    "temporal-risk":             {"addressed": <bool>, "verdict": ..., "note": ...},
    "reviewer-appetite":         {"addressed": <bool>, "verdict": ..., "note": ...},
    "methodological-readiness":  {"addressed": <bool>, "verdict": ..., "note": ...}
  },
  "papers_cited_markers": [<array of strings — every distinct citation marker as used in the input: "[P1]", "Paper 1", "Paper A3", etc.>],
  "research_questions": [<array of strings — RQs verbatim from the input (or short paraphrase if RQ is very long), max 7 items>],
  "hypotheses": [<array of strings — hypotheses verbatim (or short paraphrase if very long), max 10 items>]
}

Do NOT enumerate author names, scale names, or theoretical references — that's a downstream task. Focus on the structural fields above.

GRADE MAPPING RULES (apply these strictly):
- Letter grades: A+/A/A− → "Strong"; B+/B → "Conditional"; B− → "Conditional" if rationale positive, "Weak" if negative; C+/C → "Weak"; C− or below → "Do not pursue".
- Compound grades (e.g. "Theoretical opportunity: A−, Venue opportunity: B+"): use the LOWER of the two.
- Narrative phrasing ("strong gap", "unlikely to survive review"): map semantically.
- If no grade can be inferred from the input, set grade = "Conditional" and grade_inferred = true. Otherwise grade_inferred = false.

FAILURE MODES (the seven canonical academic-gap failure modes):
- The input may address these as named section headers, narrative weave, "reviewer risks", or in any other form. Map content SEMANTICALLY — do not require exact header matches.
- "addressed" = true only if the analysis substantively discusses that specific concern (not just mentions it in passing).
- If addressed=true: "verdict" is "passes" (no concern), "flagged" (concern raised but not fully resolved), or "fixed" (concern raised and mitigated). "note" is a <=200-char summary.
- If addressed=false: still include "verdict": "passes" (placeholder) and "note": "" — every mode must have all three fields for schema consistency.

Return ONLY the JSON object. No \`\`\`json fences. No commentary before or after.`;

const NORMALISER_SCHEMA = {
  type: 'object',
  properties: {
    grade: { type: 'string', enum: ['Strong', 'Conditional', 'Weak', 'Do not pursue'] },
    grade_rationale_one_sentence: { type: 'string' },
    grade_inferred: { type: 'boolean' },
    grade_original_form: { type: 'string' },
    gap_type: {
      type: 'string',
      enum: ['Theoretical', 'Empirical', 'Methodological', 'Application', 'Multiple'],
    },
    failure_modes: {
      type: 'object',
      properties: {
        'scope-too-narrow': { $ref: '#/$defs/failure_mode' },
        'scope-too-broad': { $ref: '#/$defs/failure_mode' },
        'theoretical-insufficiency': { $ref: '#/$defs/failure_mode' },
        'application-ceiling': { $ref: '#/$defs/failure_mode' },
        'temporal-risk': { $ref: '#/$defs/failure_mode' },
        'reviewer-appetite': { $ref: '#/$defs/failure_mode' },
        'methodological-readiness': { $ref: '#/$defs/failure_mode' },
      },
      required: [
        'scope-too-narrow',
        'scope-too-broad',
        'theoretical-insufficiency',
        'application-ceiling',
        'temporal-risk',
        'reviewer-appetite',
        'methodological-readiness',
      ],
    },
    papers_cited_markers: { type: 'array', items: { type: 'string' } },
    research_questions: { type: 'array', items: { type: 'string' } },
    hypotheses: { type: 'array', items: { type: 'string' } },
  },
  required: [
    'grade',
    'grade_rationale_one_sentence',
    'grade_inferred',
    'grade_original_form',
    'gap_type',
    'failure_modes',
    'papers_cited_markers',
    'research_questions',
    'hypotheses',
  ],
  $defs: {
    failure_mode: {
      type: 'object',
      properties: {
        addressed: { type: 'boolean' },
        verdict: {
          type: 'string',
          enum: ['passes', 'flagged', 'fixed', 'unaddressed'],
        },
        note: { type: 'string' },
      },
      required: ['addressed'],
    },
  },
};

export interface BHExtract {
  grade: 'Strong' | 'Conditional' | 'Weak' | 'Do not pursue';
  grade_rationale_one_sentence: string;
  grade_inferred: boolean;
  grade_original_form: string;
  gap_type: 'Theoretical' | 'Empirical' | 'Methodological' | 'Application' | 'Multiple';
  failure_modes: Record<
    string,
    { addressed: boolean; verdict?: 'passes' | 'flagged' | 'fixed' | 'unaddressed'; note?: string }
  >;
  papers_cited_markers: string[];
  research_questions: string[];
  hypotheses: string[];
}

/**
 * Strip ``` or ```json markdown fences if the model added them despite the
 * system-prompt instruction not to. Also strips leading/trailing whitespace.
 * The model occasionally returns `Here is the JSON:\n\`\`\`json\n{...}\n\`\`\`` —
 * we extract the JSON object from such envelopes.
 */
function stripJsonFences(text: string): string {
  let s = text.trim();
  // Common envelope: extract from first { to matching last }.
  const firstBrace = s.indexOf('{');
  const lastBrace = s.lastIndexOf('}');
  if (firstBrace !== -1 && lastBrace > firstBrace) {
    s = s.substring(firstBrace, lastBrace + 1);
  }
  return s;
}

/** Heuristic: only normalise substantive analyses, not status messages or supplementary requests. */
export function shouldNormalise(text: string): boolean {
  if (!text || text.length < MIN_CONTENT_LENGTH) return false;
  const head = text.substring(0, 200).toLowerCase();
  if (head.includes('supplementary literature request')) return false;
  if (head.startsWith('brief received')) return false;
  if (head.startsWith('starting ')) return false;
  if (head.startsWith('in progress')) return false;
  if (head.startsWith('on it')) return false;
  return true;
}

/**
 * Run the normaliser. Returns the extract, or null on any failure (logged).
 */
export async function normaliseBountyHunterOutput(text: string): Promise<BHExtract | null> {
  try {
    const result = await spawnClaudeNormaliser(text);
    return result;
  } catch (err) {
    log.error('BH normaliser invocation failed', { err: err instanceof Error ? err.message : String(err) });
    return null;
  }
}

function spawnClaudeNormaliser(bhOutput: string): Promise<BHExtract> {
  return new Promise((resolve, reject) => {
    // Constrained generation via --json-schema is too slow on large inputs
    // (90s+ timeout on 25KB BH outputs with the 7-mode schema). Rely on
    // a precise system prompt instead, and strip any markdown fence the
    // model may add despite the instruction.
    const args = [
      '-p',
      '--model',
      HAIKU_MODEL,
      '--output-format',
      'json',
      '--system-prompt',
      NORMALISER_SYSTEM_PROMPT,
      '--no-session-persistence',
      '--tools',
      '',
      '--disable-slash-commands',
      '--exclude-dynamic-system-prompt-sections',
      '--dangerously-skip-permissions',
    ];

    const proc = spawn('claude', args, {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env },
    });

    let stdout = '';
    let stderr = '';
    let timedOut = false;

    const timer = setTimeout(() => {
      timedOut = true;
      proc.kill('SIGTERM');
      reject(new Error(`Normaliser timed out after ${NORMALISER_TIMEOUT_MS}ms`));
    }, NORMALISER_TIMEOUT_MS);

    proc.stdout.on('data', (chunk: Buffer) => {
      stdout += chunk.toString('utf8');
    });
    proc.stderr.on('data', (chunk: Buffer) => {
      stderr += chunk.toString('utf8');
    });

    proc.on('error', (err: Error) => {
      if (timedOut) return;
      clearTimeout(timer);
      reject(err);
    });

    proc.on('exit', (code) => {
      if (timedOut) return;
      clearTimeout(timer);
      if (code !== 0) {
        reject(new Error(`Normaliser CLI exited ${code}: ${stderr.substring(0, 500)}`));
        return;
      }
      try {
        const wrapper = JSON.parse(stdout);
        if (wrapper.subtype !== 'success' || typeof wrapper.result !== 'string') {
          reject(new Error(`Unexpected CLI wrapper: ${JSON.stringify(wrapper).substring(0, 300)}`));
          return;
        }
        const innerJson = stripJsonFences(wrapper.result);
        const inner = JSON.parse(innerJson);
        resolve(inner as BHExtract);
      } catch (err) {
        reject(
          new Error(
            `Failed to parse normaliser output: ${err instanceof Error ? err.message : err}. stdout head: ${stdout.substring(0, 300)}`,
          ),
        );
      }
    });

    // Write the BH output to stdin so it doesn't bloat the argv (which has OS limits).
    proc.stdin.write(bhOutput);
    proc.stdin.end();
  });
}

/**
 * Persist the extract to a stable host-managed path, and build the annotation
 * block that gets appended to the outbound content.
 */
export function persistExtract(
  bhSessionId: string,
  a2aMsgId: string,
  extract: BHExtract,
): { extractPath: string; annotation: string } {
  const dir = path.join(DATA_DIR, 'normaliser-extracts', 'bountyhunter', bhSessionId);
  fs.mkdirSync(dir, { recursive: true });
  const extractPath = path.join(dir, `${a2aMsgId}.json`);
  fs.writeFileSync(extractPath, JSON.stringify(extract, null, 2) + '\n');

  const addressedCount = Object.values(extract.failure_modes).filter((m) => m.addressed).length;
  const unaddressedModes = Object.entries(extract.failure_modes)
    .filter(([, v]) => !v.addressed)
    .map(([k]) => k);

  const annotation =
    '\n\n' +
    '[system-annotation: bh-extract-ready]\n' +
    `Path: ${extractPath}\n` +
    `Grade: ${extract.grade} (mapped from "${extract.grade_original_form}"${extract.grade_inferred ? ', inferred' : ''})\n` +
    `Gap type: ${extract.gap_type}\n` +
    `Failure modes addressed: ${addressedCount}/7${unaddressedModes.length ? ' (missing: ' + unaddressedModes.join(', ') + ')' : ''}\n` +
    `Papers cited (markers): ${extract.papers_cited_markers.length ? extract.papers_cited_markers.join(', ') : '(none detected)'}\n` +
    `Research questions: ${extract.research_questions.length}; Hypotheses: ${extract.hypotheses.length}\n` +
    'Citation hallucination check: not run at normalisation; Stage 4.5 QC handles citation verification.\n' +
    '[/system-annotation]';

  return { extractPath, annotation };
}
