// Fix #1: getPublicBaseUrl promoted to module-level so it's reusable across all guards
const getPublicBaseUrl = (req) => {
  const envBase = (process.env.PUBLIC_BASE_URL || '').trim().replace(/\/+$/, '');
  if (envBase) return envBase;

  const proto = (req.headers['x-forwarded-proto'] || req.protocol || 'https')
    .toString()
    .split(',')[0]
    .trim();

  const host = (req.headers['x-forwarded-host'] || req.get('host') || '')
    .toString()
    .split(',')[0]
    .trim();

  return `${proto}://${host}`.replace(/\/+$/, '');
};

export function requireApiKey(req, res, next) {
  const auth = (req.headers.authorization || '').toString();
  if (auth.startsWith('Bearer ')) return next();

  const expected = process.env.MCP_API_KEY;
  if (!expected) return next();

  const got = req.header('x-api-key');
  if (got && got === expected) return next();

  const base = getPublicBaseUrl(req);
  const resourceMetadataUrl = `${base}/.well-known/oauth-protected-resource`;
  res.set('WWW-Authenticate', `Bearer realm="mcp", resource_metadata="${resourceMetadataUrl}"`);
  return res.status(401).json({ error: 'Unauthorized (missing/invalid x-api-key)' });
}

export function requireAllowedOrigin(req, res, next) {
  const auth = (req.headers.authorization || '').toString();
  if (auth.startsWith('Bearer ')) return next();

  const allowRaw = process.env.ALLOWED_ORIGINS || '';
  const allow = allowRaw
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  if (allow.length === 0) return next();
  if (allow.includes('*')) return next();

  const origin = req.header('origin');
  // Non-browser callers may not send Origin.
  if (!origin) return next();

  if (!allow.includes(origin)) {
    return res.status(403).json({ error: `Forbidden (Origin not allowed): ${origin}` });
  }

  return next();
}

// Fix #2 (used in mountMcp.js): GEMINI_API_KEY is now optional.
// If not set, SSE endpoints are simply open (no auth). Set the env var to lock them down.
export function requireGeminiApiKey(req, res, next) {
  const expected = (process.env.GEMINI_API_KEY || '').trim();
  // If GEMINI_API_KEY is not configured, allow the request through (open endpoint).
  if (!expected) return next();

  const auth = (req.headers.authorization || '').toString();
  const token = auth.startsWith('Bearer ') ? auth.slice('Bearer '.length).trim() : '';

  if (token !== expected) {
    return res.status(401).json({ error: 'Unauthorized (invalid or missing GEMINI_API_KEY)' });
  }

  return next();
}
