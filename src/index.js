import 'dotenv/config';
import express from 'express';
import { createMcpExpressApp } from '@modelcontextprotocol/sdk/server/express.js';

import { mountMcp } from './mcp/mountMcp.js';

const PORT = Number(process.env.PORT || 3000);
const NODE_ENV = process.env.NODE_ENV || 'development';
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || '').trim().replace(/\/+$/, '');
const IS_PUBLIC_DEPLOYMENT =
    NODE_ENV === 'production' || Boolean(PUBLIC_BASE_URL) || process.env.BIND_PUBLIC === '1';

const publicBaseHostname = (() => {
    if (!PUBLIC_BASE_URL) return '';
    try {
        return new URL(PUBLIC_BASE_URL).hostname;
    } catch (_err) {
        return '';
    }
})();

const extraAllowedHosts = [
    process.env.RENDER_EXTERNAL_HOSTNAME,
    publicBaseHostname,
    ...(process.env.ALLOWED_HOSTS || '').split(','),
]
    .map((s) => s?.trim())
    .filter(Boolean);

const mcpAppOptions =
    extraAllowedHosts.length > 0
        ? {
            host: '0.0.0.0',
            allowedHosts: [...new Set(['localhost', '127.0.0.1', '[::1]', ...extraAllowedHosts])],
        }
        : IS_PUBLIC_DEPLOYMENT
            ? { host: '0.0.0.0' }
            : { host: '127.0.0.1' };

const CORS_ALLOWED_ORIGINS = (process.env.CORS_ALLOWED_ORIGINS || '*')
    .split(',')
    .map((s) => s?.trim())
    .filter(Boolean);

const CORS_ALLOW_CREDENTIALS = (process.env.CORS_ALLOW_CREDENTIALS || '0') === '1';

const DEFAULT_CORS_HEADERS = [
    'Authorization',
    'Content-Type',
    'Accept',
    'Origin',
    'MCP-Session-Id',
    'mcp-session-id',
    'x-api-key',
].join(',');

const resolveCorsOrigin = (originHeader) => {
    if (CORS_ALLOWED_ORIGINS.includes('*')) {
        if (CORS_ALLOW_CREDENTIALS && originHeader) return originHeader;
        return '*';
    }

    if (!originHeader) return null;
    return CORS_ALLOWED_ORIGINS.includes(originHeader) ? originHeader : null;
};

const applyCorsHeaders = (req, res) => {
    const originHeader = req.header('origin');
    const corsOrigin = resolveCorsOrigin(originHeader);
    if (corsOrigin) {
        res.set('Access-Control-Allow-Origin', corsOrigin);
        if (corsOrigin !== '*') res.vary('Origin');
    }

    const requestHeaders = req.header('access-control-request-headers');
    res.set('Access-Control-Allow-Headers', requestHeaders || DEFAULT_CORS_HEADERS);
    res.set('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
    res.set('Access-Control-Expose-Headers', 'WWW-Authenticate,MCP-Session-Id,mcp-session-id');

    if (CORS_ALLOW_CREDENTIALS && corsOrigin && corsOrigin !== '*') {
        res.set('Access-Control-Allow-Credentials', 'true');
    }
};

const TENANT_ID = process.env.AZURE_TENANT_ID || '463f5aca-3098-440c-a795-9819035e156f';

const MCP_SERVICE_APP_ID =
    process.env.MCP_SERVICE_APP_ID || 'c600189c-5401-4bd7-9d45-e787222bb030';

const MCP_SCOPE =
    process.env.MCP_SCOPE || `api://${MCP_SERVICE_APP_ID}/mcp.access`;

const CHATGPT_CLIENT_ID =
    process.env.CHATGPT_CLIENT_ID || 'e206c9dc-1fd5-4f1c-97fb-e785ef875590';

const CHATGPT_CLIENT_SECRET = (process.env.CHATGPT_CLIENT_SECRET || '').trim();

const CHATGPT_REDIRECT_URIS = (
    process.env.CHATGPT_REDIRECT_URIS ||
    'https://chatgpt.com/connector_platform_oauth_redirect,https://platform.openai.com/apps-manage/oauth'
)
    .split(',')
    .map((s) => s?.trim())
    .filter(Boolean);

const REQUIRE_MCP_AUTH =
    (process.env.MCP_REQUIRE_AUTH || (NODE_ENV === 'production' ? '1' : '0')) === '1';

const OAUTH_DEBUG = (process.env.OAUTH_DEBUG || '0') === '1';

const DEFAULT_OAUTH_TOKEN_ORIGIN = (() => {
    const envOrigin = (process.env.OAUTH_TOKEN_ORIGIN || '').trim();
    if (envOrigin) return envOrigin;

    for (const redirectUri of CHATGPT_REDIRECT_URIS) {
        try {
            const u = new URL(redirectUri);
            if (u.origin === 'https://chatgpt.com' || u.origin === 'https://platform.openai.com') {
                return u.origin;
            }
        } catch (_err) {
            // Ignore malformed redirect URIs.
        }
    }

    return 'https://chatgpt.com';
})();

const AUTHORIZATION_ENDPOINT = `https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/authorize`;
const TOKEN_ENDPOINT = `https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/token`;
const JWKS_URI = `https://login.microsoftonline.com/${TENANT_ID}/discovery/v2.0/keys`;

const app = createMcpExpressApp(mcpAppOptions);

// Render/Reverse-proxy friendly URLs
app.set('trust proxy', 1);

// Keep MCP/OAuth endpoints callable from ChatGPT browser + backend probes.
app.use((req, res, next) => {
    applyCorsHeaders(req, res);
    if (req.method === 'OPTIONS') return res.status(204).end();
    return next();
});

// NOTE: createMcpExpressApp() already mounts a JSON body parser.
// Some clients (and some manual tests) may send an empty/invalid JSON body to /register.
// If the JSON parser throws, recover specifically for /register and still respond.
app.use((err, req, res, next) => {
    const isBodyParseError = err && (err.type === 'entity.parse.failed' || err instanceof SyntaxError);
    if (isBodyParseError && req && req.path === '/register') {
        try {
            applyCorsHeaders(req, res);
            const token_endpoint_auth_method = CHATGPT_CLIENT_SECRET ? 'client_secret_post' : 'none';
            const out = {
                client_id: CHATGPT_CLIENT_ID,
                token_endpoint_auth_method,
                redirect_uris: CHATGPT_REDIRECT_URIS,
                grant_types: ['authorization_code', 'refresh_token'],
                response_types: ['code'],
            };
            if (CHATGPT_CLIENT_SECRET) out.client_secret = CHATGPT_CLIENT_SECRET;
            return res.status(200).json(out);
        } catch (_e) {
            // fall through to default error handler
        }
    }
    return next(err);
});

const getPublicBaseUrl = (req) => {
    if (PUBLIC_BASE_URL) return PUBLIC_BASE_URL;
    const proto = (req.headers['x-forwarded-proto'] || req.protocol || 'https')
        .toString()
        .split(',')[0]
        .trim();
    const host = (req.headers['x-forwarded-host'] || req.get('host') || '').toString().split(',')[0].trim();
    return `${proto}://${host}`.replace(/\/+$/, '');
};

const buildProtectedResourceMetadata = (req) => {
    const base = getPublicBaseUrl(req);
    return {
        resource: `${base}`,
        authorization_servers: [base],
        scopes_supported: [MCP_SCOPE, 'offline_access'],
    };
};

const buildAuthorizationServerMetadata = (req) => {
    const base = getPublicBaseUrl(req);
    return {
        issuer: base,
        authorization_endpoint: `${base}/oauth/authorize`,
        token_endpoint: `${base}/oauth/token`,
        registration_endpoint: `${base}/register`,
        jwks_uri: JWKS_URI,
        code_challenge_methods_supported: ['S256'],
        response_types_supported: ['code'],
        grant_types_supported: ['authorization_code', 'refresh_token'],
        token_endpoint_auth_methods_supported: CHATGPT_CLIENT_SECRET
            ? ['client_secret_post', 'none']
            : ['none'],
        scopes_supported: [MCP_SCOPE, 'offline_access'],
    };
};

app.get('/health', (_req, res) => {
    res.json({
        ok: true,
        mode: {
            requireMcpAuth: REQUIRE_MCP_AUTH,
            statelessMcp: (process.env.MCP_STATELESS || (process.env.VERCEL ? '1' : '0')) === '1',
        },
    });
});

// Protected Resource Metadata (MCP requirement)
app.get(['/.well-known/oauth-protected-resource', '/mcp/.well-known/oauth-protected-resource'], (req, res) => {
    res.json(buildProtectedResourceMetadata(req));
});

// Some clients may probe a resource-specific PRM URL. Provide a compatible alias.
app.get('/.well-known/oauth-protected-resource/mcp', (req, res) => {
    const base = getPublicBaseUrl(req);
    res.json({
        resource: `${base}/mcp`,
        authorization_servers: [base],
        scopes_supported: [MCP_SCOPE, 'offline_access'],
    });
});

// Authorization Server Metadata (RFC8414-style)
app.get(['/.well-known/oauth-authorization-server', '/mcp/.well-known/oauth-authorization-server'], (req, res) => {
    res.json(buildAuthorizationServerMetadata(req));
});

// OIDC discovery (some clients prefer this endpoint)
app.get(['/.well-known/openid-configuration', '/mcp/.well-known/openid-configuration'], (req, res) => {
    res.json(buildAuthorizationServerMetadata(req));
});

// OAuth proxy endpoints (Entra v2 rejects the 'resource' parameter; ChatGPT includes it)
// We strip 'resource' and forward to Entra.
app.get('/oauth/authorize', (req, res) => {
    try {
        const forwarded = new URL(AUTHORIZATION_ENDPOINT);
        // Copy all query params except 'resource'
        for (const [k, v] of Object.entries(req.query || {})) {
            if (k === 'resource') continue;
            if (Array.isArray(v)) {
                v.forEach((vv) => forwarded.searchParams.append(k, String(vv)));
            } else if (v !== undefined && v !== null) {
                forwarded.searchParams.set(k, String(v));
            }
        }
        return res.redirect(forwarded.toString());
    } catch (err) {
        console.error('OAuth /oauth/authorize error:', err);
        return res.status(500).send('OAuth authorize proxy error');
    }
});

// Token exchange uses x-www-form-urlencoded
app.post('/oauth/token', express.urlencoded({ extended: false }), async (req, res) => {
    try {
        const body = { ...(req.body || {}) };
        delete body.resource;

        const params = new URLSearchParams();
        Object.entries(body).forEach(([k, v]) => {
            if (v === undefined || v === null) return;
            params.set(k, String(v));
        });

        const requestOrigin = (req.headers.origin || '').toString().split(',')[0].trim();
        const forwardOrigin = requestOrigin || DEFAULT_OAUTH_TOKEN_ORIGIN;
        const forwardHeaders = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'Origin': forwardOrigin,
        };

        if (OAUTH_DEBUG) {
            console.log('oauth/token request', {
                grant_type: body.grant_type || null,
                has_client_id: Boolean(body.client_id),
                has_code: Boolean(body.code),
                has_code_verifier: Boolean(body.code_verifier),
                has_refresh_token: Boolean(body.refresh_token),
                inbound_origin: requestOrigin || null,
                forward_origin: forwardOrigin,
            });
        }

        const r = await fetch(TOKEN_ENDPOINT, {
            method: 'POST',
            headers: forwardHeaders,
            body: params.toString(),
        });

        const text = await r.text();
        if (OAUTH_DEBUG || !r.ok) {
            const snippet = text?.slice(0, 500) || '';
            console.log('oauth/token response', { status: r.status, body: snippet });
        }
        res.status(r.status);
        // Forward content-type if present, else default to json
        const ct = r.headers.get('content-type') || 'application/json; charset=utf-8';
        res.set('Content-Type', ct);
        return res.send(text);
    } catch (err) {
        console.error('OAuth /oauth/token error:', err);
        return res.status(500).json({ error: 'server_error', error_description: 'Token proxy error' });
    }
});

// Dynamic Client Registration shim (Entra ID does not support RFC7591 DCR)
// We return a pre-created Entra client id (ChatGPT-MCP-Client) as a stable client.
const buildDcrResponse = () => {
    if (!CHATGPT_CLIENT_ID) {
        return {
            status: 500,
            body: {
                error: 'server_error',
                error_description: 'CHATGPT_CLIENT_ID is not configured on the server',
            },
        };
    }

    const token_endpoint_auth_method = CHATGPT_CLIENT_SECRET ? 'client_secret_post' : 'none';

    const out = {
        client_id: CHATGPT_CLIENT_ID,
        token_endpoint_auth_method,
        redirect_uris: CHATGPT_REDIRECT_URIS,
        grant_types: ['authorization_code', 'refresh_token'],
        response_types: ['code'],
    };

    if (CHATGPT_CLIENT_SECRET) {
        out.client_secret = CHATGPT_CLIENT_SECRET;
    }

    return { status: 200, body: out };
};

app.post('/register', (_req, res) => {
    try {
        const out = buildDcrResponse();
        res.status(out.status).json(out.body);
    } catch (err) {
        console.error('DCR /register error:', err);
        res.status(500).json({ error: 'server_error', error_description: 'Unhandled error in /register' });
    }
});

// Helpful for manual browser navigation (GET will be used in the address bar)
app.get('/register', (_req, res) => {
    try {
        const out = buildDcrResponse();
        res.status(out.status).json(out.body);
    } catch (err) {
        console.error('DCR /register (GET) error:', err);
        res.status(500).json({ error: 'server_error', error_description: 'Unhandled error in /register' });
    }
});

// Require auth for MCP calls (ChatGPT expects 401 + WWW-Authenticate challenge)
app.use('/mcp', (req, res, next) => {
    if (!REQUIRE_MCP_AUTH) return next();
    if (req.method === 'OPTIONS') return next();

    const auth = (req.headers.authorization || '').toString();
    if (auth.startsWith('Bearer ')) return next();

    const base = getPublicBaseUrl(req);
    res.set(
        'WWW-Authenticate',
        `Bearer realm="mcp", resource_metadata="${base}/.well-known/oauth-protected-resource"`
    );
    res.status(401).json({ error: 'unauthorized' });
});

mountMcp(app);

const server = app.listen(PORT, () => {
    console.log(`HTTP listening on port ${PORT}`);
    console.log(`MCP   -> /mcp`);
    if (extraAllowedHosts.length > 0) {
        console.log(`MCP allowed hosts: ${mcpAppOptions.allowedHosts.join(', ')}`);
    }
});

// Fix #3: express's listen callback doesn't receive an error argument.
// Attach an error event handler to properly catch port conflicts / startup failures.
server.on('error', (err) => {
    console.error('Failed to start server:', err);
    process.exit(1);
});
