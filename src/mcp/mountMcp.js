import { randomUUID } from 'node:crypto';
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { isInitializeRequest } from '@modelcontextprotocol/sdk/types.js';

import { requireApiKey, requireAllowedOrigin, requireGeminiApiKey } from '../auth.js';
import { buildMcpServer } from './server.js';

const sseTransports = new Map();

function getBearerToken(req) {
  const auth = (req.headers.authorization || '').toString();
  if (!auth.startsWith('Bearer ')) return '';
  return auth.slice('Bearer '.length).trim();
}

export function mountMcp(app) {
  const useStatelessMode =
    (process.env.MCP_STATELESS || (process.env.VERCEL ? '1' : '0')) === '1';
  const transports = new Map();
  const sessionTokens = new Map();

  app.post('/mcp', requireApiKey, requireAllowedOrigin, async (req, res) => {
    // Compat: The MCP SDK v1.5 StreamableHTTPServerTransport is strictly expecting 
    // application/json AND text/event-stream in the Accept header. 
    // ChatGPT and some generic REST clients (like HARPA) don't send this correctly, causing a 406 Not Acceptable.
    if (!req.headers.accept || req.headers.accept.includes('*/*') || req.headers.accept === 'application/json') {
      req.headers.accept = 'application/json, text/event-stream';
      if (req.rawHeaders) {
        const idx = req.rawHeaders.findIndex(h => h.toLowerCase() === 'accept');
        if (idx !== -1) req.rawHeaders[idx + 1] = 'application/json, text/event-stream';
        else req.rawHeaders.push('Accept', 'application/json, text/event-stream');
      }
    }

    try {
      if (useStatelessMode) {
        const inboundToken = getBearerToken(req);

        // If the Bearer token matches our configured API keys, OR if it's completely missing
        // the standard JWT dot-notation structure (meaning it's just a generic API key
        // like from HARPA rather than a real Microsoft token from ChatGPT), we auto-fallback 
        // to headless 'client_credentials' mode to prevent OBO token exchange crashes.
        const geminiKey = (process.env.GEMINI_API_KEY || '').trim();
        const mcpKey = (process.env.MCP_API_KEY || '').trim();
        const isBotClient =
          (geminiKey && inboundToken === geminiKey) ||
          (mcpKey && inboundToken === mcpKey) ||
          (inboundToken && !inboundToken.includes('.'));

        const transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: undefined,
          enableJsonResponse: true,
        });
        const server = isBotClient
          ? buildMcpServer({ authMode: 'client_credentials' })
          : buildMcpServer({ getInboundAccessToken: () => inboundToken });
        await server.connect(transport);
        await transport.handleRequest(req, res, req.body);
        res.on('close', () => {
          transport.close();
          server.close();
        });
        return;
      }

      const sessionId = req.headers['mcp-session-id'];
      let transport = sessionId ? transports.get(String(sessionId)) : undefined;

      if (!transport && !sessionId && isInitializeRequest(req.body)) {
        let initializedSessionId = '';
        const initialInboundToken = getBearerToken(req);

        transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => randomUUID(),
          enableJsonResponse: true,
          onsessioninitialized: (sid) => {
            initializedSessionId = sid;
            transports.set(sid, transport);
            if (initialInboundToken) {
              sessionTokens.set(sid, initialInboundToken);
            }
          },
        });

        const server = buildMcpServer({
          getInboundAccessToken: () =>
            (initializedSessionId ? sessionTokens.get(initializedSessionId) : undefined) || initialInboundToken,
        });
        await server.connect(transport);

        await transport.handleRequest(req, res, req.body);
        return;
      }

      if (!transport) {
        res.status(400).json({
          jsonrpc: '2.0',
          error: { code: -32000, message: 'Bad Request: No valid session ID provided' },
          id: null,
        });
        return;
      }

      const incomingToken = getBearerToken(req);
      if (incomingToken) {
        sessionTokens.set(String(sessionId), incomingToken);
      }

      await transport.handleRequest(req, res, req.body);
    } catch (err) {
      console.error('MCP error:', err);
      if (!res.headersSent) {
        res.status(500).json({
          jsonrpc: '2.0',
          error: { code: -32603, message: 'Internal server error' },
          id: null,
        });
      }
    }
  });

  app.get('/mcp/sse', requireGeminiApiKey, async (req, res) => {
    try {
      const transport = new SSEServerTransport('/mcp/messages', res);
      const server = buildMcpServer({ authMode: 'client_credentials' });
      await server.connect(transport);

      sseTransports.set(transport.sessionId, transport);

      req.on('close', () => {
        sseTransports.delete(transport.sessionId);
        server.close();
      });

    } catch (err) {
      console.error('MCP SSE Init error:', err);
      if (!res.headersSent) {
        res.status(500).send('Internal Server Error init SSE');
      }
    }
  });

  app.post('/mcp/messages', requireGeminiApiKey, async (req, res) => {
    try {
      const sessionId = req.query.sessionId;
      const transport = sseTransports.get(sessionId);

      if (!transport) {
        return res.status(404).json({ error: 'SSE session not found' });
      }

      await transport.handlePostMessage(req, res, req.body);
    } catch (err) {
      console.error('MCP messages error:', err);
      if (!res.headersSent) {
        res.status(500).json({ error: 'Internal Server Error handling message' });
      }
    }
  });

  app.get('/mcp', requireApiKey, requireAllowedOrigin, (_req, res) => {
    res.status(405).set('Allow', 'POST').send('Method Not Allowed');
  });

  app.delete('/mcp', requireApiKey, requireAllowedOrigin, (_req, res) => {
    res.status(405).set('Allow', 'POST').send('Method Not Allowed');
  });
}

