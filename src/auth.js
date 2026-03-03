export function requireApiKey(req, res, next) {
  const auth = (req.headers.authorization || '').toString();
  if (auth.startsWith('Bearer ')) return next();

  const expected = process.env.MCP_API_KEY;
  if (!expected) return next();

  const got = req.header('x-api-key');
  if (got && got === expected) return next();

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
  if (!origin) return next();

  if (!allow.includes(origin)) {
    return res.status(403).json({ error: `Forbidden (Origin not allowed): ${origin}` });
  }

  return next();
}

export function requireGeminiApiKey(req, res, next) {
  const expected = process.env.GEMINI_API_KEY;
  if (!expected) {
    return res.status(500).json({ error: 'Server misconfigured. GEMINI_API_KEY is not set.' });
  }

  const auth = (req.headers.authorization || '').toString();
  const token = auth.startsWith('Bearer ') ? auth.slice('Bearer '.length).trim() : '';

  if (token !== expected) {
    return res.status(401).json({ error: 'Unauthorized (invalid or missing GEMINI_API_KEY)' });
  }

  return next();
}
