/**
 * discovery.js — pure helpers for turning Google's `GET /v1beta/models`
 * response into a chat-model catalog diff. Split out of server.js so this
 * logic (which is just string/array munging) is testable without booting
 * the whole proxy or hitting the network — see test.js.
 */

// Non-chat / non-text model families Google lists alongside the Gemini chat
// lineup (embeddings, TTS, image/video gen, live/audio, robotics, etc.) —
// never useful as an OpenCode chat completion target.
const EXCLUDE_RE = /embedding|tts|image|live|audio|dialog|robotics|computer-use|aqa|transcribe/i;

/* Google's models array -> bare chat-model ids ("models/gemini-x" -> "gemini-x").
 * Keeps only real Gemini chat models: must support generateContent, must not
 * match EXCLUDE_RE, and drops "-latest" aliases (they duplicate a concrete
 * dated/versioned id already in the list). */
export function filterChatModels(googleModelsArray) {
  if (!Array.isArray(googleModelsArray)) return [];
  const out = [];
  for (const m of googleModelsArray) {
    const name = m && typeof m.name === 'string' ? m.name : null;
    if (!name) continue;
    const id = name.startsWith('models/') ? name.slice('models/'.length) : name;
    if (!id.startsWith('gemini-')) continue;
    if (id.endsWith('-latest')) continue;
    if (EXCLUDE_RE.test(id)) continue;
    const methods = Array.isArray(m.supportedGenerationMethods) ? m.supportedGenerationMethods : [];
    if (!methods.includes('generateContent')) continue;
    out.push(id);
  }
  return out;
}

const TIER_RANK = { pro: 0, flash: 1, 'flash-lite': 2 };
function tierOf(id) {
  if (/flash-lite/i.test(id)) return 'flash-lite';
  if (/flash/i.test(id)) return 'flash';
  if (/pro/i.test(id)) return 'pro';
  return null;
}
function versionOf(id) {
  const m = /gemini-(\d+(?:\.\d+)?)/.exec(id);
  return m ? parseFloat(m[1]) : null;
}

/* Newest-first sort: by version number (unknown/unparseable versions sort
 * last), then pro > flash > flash-lite within the same version, stable
 * otherwise (preserves input order for ties). */
export function sortByVersionDesc(ids) {
  return ids
    .map((id, i) => ({ id, i, version: versionOf(id), tier: tierOf(id) }))
    .sort((a, b) => {
      if (a.version == null && b.version == null) return a.i - b.i;
      if (a.version == null) return 1;
      if (b.version == null) return -1;
      if (a.version !== b.version) return b.version - a.version;
      const ra = TIER_RANK[a.tier] ?? 99, rb = TIER_RANK[b.tier] ?? 99;
      if (ra !== rb) return ra - rb;
      return a.i - b.i;
    })
    .map(x => x.id);
}

/* Catalog diff: ids Google now lists that weren't in the previous catalog
 * ("added"), and ids that were in the previous catalog but Google no longer
 * lists ("removed"). */
export function diffCatalog(previousIds, discoveredIds) {
  const prevSet = new Set(previousIds);
  const discSet = new Set(discoveredIds);
  return {
    added: discoveredIds.filter(id => !prevSet.has(id)),
    removed: previousIds.filter(id => !discSet.has(id)),
  };
}
