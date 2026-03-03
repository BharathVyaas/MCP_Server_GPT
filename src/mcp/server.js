import { z } from 'zod';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';

const TENANT_ID = (process.env.AZURE_TENANT_ID || '').trim();
const MCP_SERVICE_APP_ID = (process.env.MCP_SERVICE_APP_ID || '').trim();
const MCP_SERVICE_APP_CLIENT_SECRET = (
  process.env.MCP_SERVICE_APP_CLIENT_SECRET ||
  process.env.MCP_SERVICE_CLIENT_SECRET ||
  process.env.AZURE_CLIENT_SECRET ||
  ''
).trim();

const DATAVERSE_URL = (process.env.DATAVERSE_URL || '').trim().replace(/\/+$/, '');
const DATAVERSE_API_VERSION = (process.env.DATAVERSE_API_VERSION || 'v9.2').trim();
const DATAVERSE_SCOPE = (
  process.env.DATAVERSE_SCOPE ||
  (DATAVERSE_URL ? `${DATAVERSE_URL}/user_impersonation` : '')
).trim();
const DATAVERSE_DEBUG = (process.env.DATAVERSE_DEBUG || '0') === '1';

const OBO_TOKEN_ENDPOINT = TENANT_ID
  ? `https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/token`
  : '';
const DATAVERSE_API_BASE = DATAVERSE_URL
  ? `${DATAVERSE_URL}/api/data/${DATAVERSE_API_VERSION}`
  : '';

const toJsonResponse = (payload) => ({
  content: [{ type: 'text', text: JSON.stringify(payload, null, 2) }],
  structuredContent: payload,
});

const toErrorResponse = (message) => ({
  isError: true,
  content: [{ type: 'text', text: message }],
});

const ensureDataverseConfig = () => {
  const missing = [];
  if (!TENANT_ID) missing.push('AZURE_TENANT_ID');
  if (!MCP_SERVICE_APP_ID) missing.push('MCP_SERVICE_APP_ID');
  if (!MCP_SERVICE_APP_CLIENT_SECRET) missing.push('MCP_SERVICE_APP_CLIENT_SECRET');
  if (!DATAVERSE_URL) missing.push('DATAVERSE_URL');
  if (!DATAVERSE_SCOPE) missing.push('DATAVERSE_SCOPE');
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
};

const toErrorText = (status, payload) => {
  if (!payload) return `status=${status}`;
  if (typeof payload === 'string') return payload;
  return payload.error_description || payload.error?.message || payload.error || `status=${status}`;
};

const parseMaybeJson = (text) => {
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (_err) {
    return text;
  }
};

const normalizeTable = (table) => {
  const cleaned = String(table || '').trim().replace(/^\/+/, '').replace(/\/+$/, '');
  if (!cleaned) throw new Error('table is required');
  return cleaned;
};

const normalizeRowId = (id) => {
  const cleaned = String(id || '').trim().replace(/^\{/, '').replace(/\}$/, '');
  if (!cleaned) throw new Error('id is required');
  return cleaned;
};

const normalizeLogicalName = (name) => {
  const cleaned = String(name || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '_')
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+/, '')
    .replace(/_+$/, '');
  if (!cleaned) throw new Error('logicalName is required');
  if (!cleaned.includes('_')) {
    throw new Error('logicalName must include a publisher prefix, e.g. cr0f1_project');
  }
  return cleaned;
};

const toSchemaName = (logicalName) => {
  const parts = logicalName.split('_').filter(Boolean);
  if (parts.length === 0) return logicalName;
  const [prefix, ...rest] = parts;
  const tail = rest.map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join('');
  return tail ? `${prefix}_${tail}` : prefix;
};

async function exchangeTokenOnBehalfOf(inboundAccessToken) {
  ensureDataverseConfig();
  if (!inboundAccessToken) {
    throw new Error('Missing incoming Bearer token. ChatGPT must call /mcp with Authorization: Bearer <token>.');
  }

  const params = new URLSearchParams({
    client_id: MCP_SERVICE_APP_ID,
    client_secret: MCP_SERVICE_APP_CLIENT_SECRET,
    grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
    requested_token_use: 'on_behalf_of',
    assertion: inboundAccessToken,
    scope: DATAVERSE_SCOPE,
  });

  const response = await fetch(OBO_TOKEN_ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/json',
    },
    body: params.toString(),
  });

  const text = await response.text();
  const payload = parseMaybeJson(text);
  if (!response.ok || !payload?.access_token) {
    throw new Error(`Dataverse OBO token exchange failed (${response.status}): ${toErrorText(response.status, payload)}`);
  }

  return payload.access_token;
}

async function getClientCredentialsToken() {
  ensureDataverseConfig();

  const params = new URLSearchParams({
    client_id: MCP_SERVICE_APP_ID,
    client_secret: MCP_SERVICE_APP_CLIENT_SECRET,
    grant_type: 'client_credentials',
    scope: DATAVERSE_URL ? `${DATAVERSE_URL}/.default` : '',
  });

  const response = await fetch(OBO_TOKEN_ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/json',
    },
    body: params.toString(),
  });

  const text = await response.text();
  const payload = parseMaybeJson(text);
  if (!response.ok || !payload?.access_token) {
    throw new Error(`Dataverse client_credentials token exchange failed (${response.status}): ${toErrorText(response.status, payload)}`);
  }

  return payload.access_token;
}

async function callDataverse(accessToken, { method = 'GET', path, query, headers, body }) {
  ensureDataverseConfig();

  const normalizedPath = String(path || '').replace(/^\/+/, '');
  const isMetadataPath = /^(EntityDefinitions|RelationshipDefinitions|GlobalOptionSetDefinitions)\b/i
    .test(normalizedPath);
  const url = new URL(normalizedPath, `${DATAVERSE_API_BASE}/`);
  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null || value === '') continue;
      url.searchParams.set(key, String(value));
    }
  }

  const requestHeaders = {
    'Authorization': `Bearer ${accessToken}`,
    'Accept': 'application/json',
    'OData-Version': '4.0',
    'OData-MaxVersion': '4.0',
    ...(isMetadataPath ? { 'ConsistencyLevel': 'eventual' } : {}),
    ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
    ...(headers || {}),
  };

  const response = await fetch(url, {
    method,
    headers: requestHeaders,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  const text = await response.text();
  const payload = parseMaybeJson(text);
  if (!response.ok) {
    throw new Error(`Dataverse API failed (${response.status}): ${toErrorText(response.status, payload)}`);
  }

  if (DATAVERSE_DEBUG) {
    console.log('dataverse request ok', { method, url: url.toString(), status: response.status });
  }

  return {
    status: response.status,
    data: payload,
    etag: response.headers.get('etag') || undefined,
    entityId: response.headers.get('odata-entityid') || undefined,
  };
}

const runWithDataverseToken = async (getInboundAccessToken, authMode, fn) => {
  try {
    let dataverseAccessToken;
    if (authMode === 'client_credentials') {
      dataverseAccessToken = await getClientCredentialsToken();
    } else {
      const inboundAccessToken = await Promise.resolve(getInboundAccessToken?.());
      dataverseAccessToken = await exchangeTokenOnBehalfOf(inboundAccessToken);
    }
    const output = await fn(dataverseAccessToken);
    return toJsonResponse(output);
  } catch (err) {
    return toErrorResponse(err?.message || 'Unexpected Dataverse error');
  }
};

export function buildMcpServer({ getInboundAccessToken, authMode = 'obo' } = {}) {
  const server = new McpServer(
    { name: 'mcp-dataverse', version: '0.2.0' },
    { capabilities: { logging: {} } }
  );

  server.registerTool(
    'dataverse_whoami',
    {
      title: 'Dataverse WhoAmI',
      description: 'Validate Dataverse connectivity and return user identifiers.',
      inputSchema: {},
    },
    async () => runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
      const result = await callDataverse(token, { method: 'GET', path: 'WhoAmI()' });
      return result;
    })
  );

  server.registerTool(
    'dataverse_list_tables',
    {
      title: 'List Dataverse Tables',
      description: 'List Dataverse table metadata (entity definitions). Use this tool to find the exact EntitySetName if you only know the display name.',
      inputSchema: {
        top: z.number().int().min(1).max(500).optional().default(100),
        customOnly: z.boolean().optional().default(false),
        logicalNameContains: z.string().optional().describe('Filter by logical name substring'),
      },
    },
    async ({ top = 100, customOnly = false, logicalNameContains }) =>
      runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
        const result = await callDataverse(token, {
          method: 'GET',
          path: 'EntityDefinitions',
          query: {
            $select: [
              'LogicalName',
              'SchemaName',
              'EntitySetName',
              'PrimaryIdAttribute',
              'PrimaryNameAttribute',
              'IsCustomEntity',
            ].join(','),
          },
        });

        if (!result?.data?.value || !Array.isArray(result.data.value)) {
          return result;
        }

        let filtered = result.data.value;

        if (customOnly) {
          filtered = filtered.filter((row) => row?.IsCustomEntity === true);
        }

        if (logicalNameContains) {
          const needle = String(logicalNameContains).toLowerCase();
          filtered = filtered.filter((row) => {
            const logical = String(row?.LogicalName || '').toLowerCase();
            const schema = String(row?.SchemaName || '').toLowerCase();
            const entitySet = String(row?.EntitySetName || '').toLowerCase();
            return logical.includes(needle) || schema.includes(needle) || entitySet.includes(needle);
          });
        }

        const limited = Number.isFinite(top) && top > 0 ? filtered.slice(0, top) : filtered;

        return {
          ...result,
          data: {
            ...result.data,
            value: limited,
          },
        };
      })
  );

  server.registerTool(
    'dataverse_list_rows',
    {
      title: 'List Dataverse Rows',
      description: 'List rows from a Dataverse table (entity set name). IMPORTANT: If you do not know the exact fully-prefixed EntitySetName (e.g., all other prefix that are available in our environment one after another with _employees), you MUST use dataverse_list_tables first to find it before calling this tool!',
      inputSchema: {
        table: z.string().describe('Dataverse entity set name, e.g. accounts or cr123_temp_employees. MUST include prefix if custom.'),
        select: z.array(z.string()).optional().describe('Columns to return. CRITICAL: Custom columns MUST include the publisher prefix in OData queries (e.g. cr123_temp_email, NOT temp_email).'),
        filter: z.string().optional().describe("OData $filter expression. CRITICAL: Custom fields MUST include prefix (e.g. cr123_status eq 1)."),
        orderBy: z.string().optional().describe("OData $orderby expression. CRITICAL: Custom fields MUST include prefix (e.g. cr123_name asc)."),
        top: z.number().int().min(1).max(500).optional().default(25),
        expand: z.string().optional().describe("OData $expand expression"),
        count: z.boolean().optional().default(false),
      },
    },
    async ({ table, select, filter, orderBy, top = 25, expand, count = false }) =>
      runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
        const entitySet = normalizeTable(table);
        const query = {
          $top: top,
          ...(select?.length ? { $select: select.join(',') } : {}),
          ...(filter ? { $filter: filter } : {}),
          ...(orderBy ? { $orderby: orderBy } : {}),
          ...(expand ? { $expand: expand } : {}),
          ...(count ? { $count: 'true' } : {}),
        };
        return callDataverse(token, { method: 'GET', path: entitySet, query });
      })
  );

  server.registerTool(
    'dataverse_create_table',
    {
      title: 'Create Dataverse Table',
      description: 'Create a custom Dataverse table (EntityDefinition). Note: newer Dataverse tenants may require an explicit PrimaryAttribute. If you receive the error "Required field \'PrimaryAttribute\' is missing for CreateEntity", provide a `primaryNameLogicalName` or adjust the tool implementation to include the generated primary attribute. This helper will automatically build a string primary attribute using your logical name if omitted.',
      inputSchema: {
        logicalName: z.string().describe('Custom logical name with prefix, e.g. cr0f1_project'),
        displayName: z.string().describe('Display name, e.g. Project'),
        displayCollectionName: z.string().optional().describe('Plural display name, e.g. Projects'),
        primaryNameLogicalName: z.string().optional().describe('Primary name attribute logical name'),
        primaryNameDisplayName: z.string().optional().describe('Primary name label, default Name'),
        ownershipType: z.enum(['UserOwned', 'OrganizationOwned']).optional().default('UserOwned'),
        description: z.string().optional(),
        primaryNameMaxLength: z.number().int().min(10).max(4000).optional().default(200),
        publishAfterCreate: z.boolean().optional().default(true),
        item: z.string().optional().describe('JSON array of custom columns without prefixes, e.g. \'[{"name":"First Name","type":"String"}]\'')
      },
    },
    async ({
      logicalName,
      displayName,
      displayCollectionName,
      primaryNameLogicalName,
      primaryNameDisplayName = 'Name',
      ownershipType = 'UserOwned',
      description = '',
      primaryNameMaxLength = 200,
      publishAfterCreate = true,
      item = [],
    }) =>
      runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
        const normalizedLogicalName = normalizeLogicalName(logicalName);
        const schemaName = toSchemaName(normalizedLogicalName);


        const label = String(displayName || '').trim();
        if (!label) throw new Error('displayName is required');
        const pluralLabel = String(displayCollectionName || `${label}s`).trim();

        const makeLabel = (text) => ({
          '@odata.type': 'Microsoft.Dynamics.CRM.Label',
          LocalizedLabels: [
            {
              '@odata.type': 'Microsoft.Dynamics.CRM.LocalizedLabel',
              Label: text,
              LanguageCode: 1033,
            },
          ],
        });

        // 1) Create Entity. Dataverse requires a PrimaryAttribute on CreateEntity; build one here so we don't hit
        //    the "Required field 'PrimaryAttribute' is missing for CreateEntity" error. We base the schema on the
        //    logical name and either use the provided value (when prefixed correctly) or the conventional
        //    <logicalName>name.
        const forcedPrimaryName = `${normalizedLogicalName}name`;
        const primaryAttributeLogicalName = primaryNameLogicalName && primaryNameLogicalName.startsWith(normalizedLogicalName)
          ? String(primaryNameLogicalName).trim().toLowerCase().replace(/[^a-z0-9_]/g, '_')
          : forcedPrimaryName;

        const primaryAttribute = {
          '@odata.type': 'Microsoft.Dynamics.CRM.StringAttributeMetadata',
          LogicalName: primaryAttributeLogicalName,
          SchemaName: toSchemaName(primaryAttributeLogicalName),
          DisplayName: makeLabel(primaryNameDisplayName),
          RequiredLevel: { Value: 'None' },
          MaxLength: primaryNameMaxLength,
          FormatName: { Value: 'Text' },
          IsPrimaryName: true,
        };

        const primaryIdLogicalName = `${normalizedLogicalName}id`;

        const entityBody = {
          '@odata.type': 'Microsoft.Dynamics.CRM.EntityMetadata',
          LogicalName: normalizedLogicalName,
          SchemaName: schemaName,
          DisplayName: makeLabel(label),
          DisplayCollectionName: makeLabel(pluralLabel),
          Description: makeLabel(description || `${label} table`),
          OwnershipType: ownershipType,
          IsActivity: false,
          HasActivities: false,
          HasNotes: true,
          // Dataverse expects new entity attributes in the Attributes collection. Some
          // tenants reject a nested PrimaryAttribute object. Provide the primary
          // attribute inside Attributes and set PrimaryNameAttribute to the logical
          // name string so the CreateEntity contract is satisfied.
          Attributes: [primaryAttribute],
          // Set PrimaryNameAttribute (string) and include the primary attribute
          // inside the Attributes collection. Do NOT send a nested
          // `PrimaryAttribute` object or unsupported top-level `PrimaryAttribute`/
          // `PrimaryIdAttribute` properties — Dataverse will reject them.
          PrimaryNameAttribute: primaryAttributeLogicalName,
        };

        if (DATAVERSE_DEBUG) {
          console.log('[dataverse_create_table] step 1 entity body:', JSON.stringify(entityBody));
        }

        let createEntityRes;
        try {
          createEntityRes = await callDataverse(token, {
            method: 'POST',
            path: 'EntityDefinitions',
            body: entityBody,
          });
        } catch (err) {
          const errMsg = err?.message || String(err || '');
          // If Dataverse complains PrimaryAttribute is missing, try a fallback
          // payload that includes a nested PrimaryAttribute object (some
          // tenants accept this shape). If the fallback also fails, return
          // both attempts for debugging.
          if (/PrimaryAttribute/.test(errMsg)) {
            const altBody = Object.assign({}, entityBody, { PrimaryAttribute: primaryAttribute });
            try {
              const altRes = await callDataverse(token, {
                method: 'POST',
                path: 'EntityDefinitions',
                body: altBody,
              });
              // success on fallback
              createEntityRes = altRes;
            } catch (altErr) {
              return {
                isError: true,
                content: [{ type: 'text', text: `Dataverse CreateEntity failed (primary attempt and fallback): ${errMsg}; fallback: ${altErr?.message || String(altErr)}` }],
                structuredContent: {
                  error: errMsg,
                  attempt: {
                    primary: { requestBody: entityBody, error: errMsg },
                    fallback: { requestBody: altBody, error: altErr?.message || String(altErr) },
                  },
                },
              };
            }
          } else {
            return {
              isError: true,
              content: [{ type: 'text', text: `Dataverse CreateEntity failed: ${errMsg}` }],
              structuredContent: {
                error: errMsg,
                requestBody: entityBody,
              },
            };
          }
        }

        let parsedItem = [];
        if (typeof item === 'string' && item.trim()) {
          try {
            parsedItem = JSON.parse(item);
          } catch (e) {
            console.warn('[dataverse_create_table] failed to parse item array:', e.message);
          }
        } else if (Array.isArray(item)) {
          parsedItem = item;
        }

        // 2) Sequentially create any custom fields requested in the 'item' array
        if (Array.isArray(parsedItem) && parsedItem.length > 0) {
          for (const col of parsedItem) {
            const colName = String(col.name || '').trim();
            const colType = String(col.type || '').trim().toLowerCase();
            if (!colName || !colType) continue;

            const colLogicalName = `${normalizedLogicalName.split('_')[0]}_${colName.toLowerCase().replace(/[^a-z0-9_]/g, '_')}`;
            let attrBody = null;

            if (colType === 'choice') {
              attrBody = {
                '@odata.type': 'Microsoft.Dynamics.CRM.PicklistAttributeMetadata',
                LogicalName: colLogicalName,
                SchemaName: toSchemaName(colLogicalName),
                DisplayName: makeLabel(colName),
                Description: makeLabel(`${colName} choice column`),
                OptionSet: {
                  Options: (col.choices || []).map((c, i) => ({
                    Value: c.value || (100000000 + i),
                    Label: makeLabel(c.label || String(c.value))
                  }))
                }
              };
            } else if (colType === 'lookup') {
              const relatedTable = col.relatedtable || 'contact';
              attrBody = {
                '@odata.type': 'Microsoft.Dynamics.CRM.LookupAttributeMetadata',
                LogicalName: colLogicalName,
                SchemaName: toSchemaName(colLogicalName),
                DisplayName: makeLabel(colName),
                Description: makeLabel(`${colName} lookup to ${relatedTable}`),
                RequiredLevel: { Value: col.required ? 'ApplicationRequired' : 'None' },
                Targets: [relatedTable],
                FormatName: { Value: 'Lookup' }
              };
            } else if (colType === 'string') {
              attrBody = {
                '@odata.type': 'Microsoft.Dynamics.CRM.StringAttributeMetadata',
                LogicalName: colLogicalName,
                SchemaName: toSchemaName(colLogicalName),
                DisplayName: makeLabel(colName),
                Description: makeLabel(`${colName} text column`),
                RequiredLevel: { Value: col.required ? 'ApplicationRequired' : 'None' },
                MaxLength: col.maxLength || 100,
                FormatName: { Value: 'Text' }
              };
            }

            if (attrBody) {
              if (DATAVERSE_DEBUG) {
                console.log(`[dataverse_create_table] step 2 creating column ${colName}:`, JSON.stringify(attrBody));
              }
              try {
                await callDataverse(token, {
                  method: 'POST',
                  path: `EntityDefinitions(LogicalName='${normalizedLogicalName}')/Attributes`,
                  body: attrBody,
                });
              } catch (err) {
                console.warn(`[dataverse_create_table] Warning: Failed to create custom column ${colName}: ${err.message}`);
              }
            }
          }
        }

        // 3) Publish
        if (publishAfterCreate) {
          try {
            await callDataverse(token, {
              method: 'POST',
              path: 'PublishAllXml',
              body: {},
            });
          } catch (publishErr) {
            return {
              ...createEntityRes,
              publish: { ok: false, error: publishErr.message }
            };
          }
        }

        return {
          ...createEntityRes,
          publish: publishAfterCreate ? { ok: true } : { ok: false, skipped: true },
        };
      })
  );

  server.registerTool(
    'dataverse_get_row',
    {
      title: 'Get Dataverse Row',
      description: 'Get one Dataverse row by GUID.',
      inputSchema: {
        table: z.string().describe('Dataverse entity set name'),
        id: z.string().describe('Dataverse row GUID (with or without braces)'),
        select: z.array(z.string()).optional(),
        expand: z.string().optional(),
      },
    },
    async ({ table, id, select, expand }) =>
      runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
        const entitySet = normalizeTable(table);
        const rowId = normalizeRowId(id);
        const query = {
          ...(select?.length ? { $select: select.join(',') } : {}),
          ...(expand ? { $expand: expand } : {}),
        };
        return callDataverse(token, {
          method: 'GET',
          path: `${entitySet}(${encodeURIComponent(rowId)})`,
          query,
        });
      })
  );

  server.registerTool(
    'dataverse_create_row',
    {
      title: 'Create Dataverse Row',
      description: 'Create a Dataverse row in the specified table.',
      inputSchema: {
        table: z.string().describe('Dataverse entity set name'),
        data: z.record(z.string(), z.unknown()).describe('JSON payload for the new row'),
        returnRepresentation: z.boolean().optional().default(true),
      },
    },
    async ({ table, data, returnRepresentation = true }) =>
      runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
        const entitySet = normalizeTable(table);
        const headers = returnRepresentation ? { Prefer: 'return=representation' } : undefined;
        return callDataverse(token, {
          method: 'POST',
          path: entitySet,
          headers,
          body: data,
        });
      })
  );

  server.registerTool(
    'dataverse_update_row',
    {
      title: 'Update Dataverse Row',
      description: 'Patch a Dataverse row by GUID.',
      inputSchema: {
        table: z.string().describe('Dataverse entity set name'),
        id: z.string().describe('Dataverse row GUID'),
        data: z.record(z.string(), z.unknown()).describe('Patch payload'),
        ifMatch: z.string().optional().default('*').describe('ETag precondition, default *'),
      },
    },
    async ({ table, id, data, ifMatch = '*' }) =>
      runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
        const entitySet = normalizeTable(table);
        const rowId = normalizeRowId(id);
        return callDataverse(token, {
          method: 'PATCH',
          path: `${entitySet}(${encodeURIComponent(rowId)})`,
          headers: { 'If-Match': ifMatch },
          body: data,
        });
      })
  );

  server.registerTool(
    'dataverse_delete_row',
    {
      title: 'Delete Dataverse Row',
      description: 'Delete a Dataverse row by GUID.',
      inputSchema: {
        table: z.string().describe('Dataverse entity set name'),
        id: z.string().describe('Dataverse row GUID'),
        ifMatch: z.string().optional().default('*').describe('ETag precondition, default *'),
      },
    },
    async ({ table, id, ifMatch = '*' }) =>
      runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
        const entitySet = normalizeTable(table);
        const rowId = normalizeRowId(id);
        return callDataverse(token, {
          method: 'DELETE',
          path: `${entitySet}(${encodeURIComponent(rowId)})`,
          headers: { 'If-Match': ifMatch },
        });
      })
  );

  server.registerTool(
    'dataverse_fetch_xml',
    {
      title: 'Execute FetchXML',
      description: 'Execute a Dataverse FetchXML query.',
      inputSchema: {
        table: z.string().describe('Entity set name (e.g. accounts)'),
        fetchXml: z.string().describe('Raw FetchXML string'),
      },
    },
    async ({ table, fetchXml }) =>
      runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
        const entitySet = normalizeTable(table);
        return callDataverse(token, {
          method: 'GET',
          path: entitySet,
          query: { fetchXml },
        });
      })
  );

  server.registerTool(
    'dataverse_execute_action',
    {
      title: 'Execute Action/Custom API',
      description: 'Execute a Dataverse unbound Action or Custom API.',
      inputSchema: {
        actionName: z.string().describe('Name of the action (e.g. cr7b_MyCustomApi)'),
        payload: z.record(z.string(), z.unknown()).optional().describe('JSON payload/parameters for the action'),
      },
    },
    async ({ actionName, payload }) =>
      runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
        const action = String(actionName || '').trim().replace(/^\/+/, '');
        if (!action) throw new Error('actionName is required');
        return callDataverse(token, {
          method: 'POST',
          path: action,
          body: payload || {},
        });
      })
  );

  server.registerTool(
    'dataverse_list_relationships',
    {
      title: 'List Table Relationships',
      description: 'Fetch Relationship metadata (1:N, N:1, N:N) for a table.',
      inputSchema: {
        logicalName: z.string().describe('Entity logical name (e.g. account)'),
      },
    },
    async ({ logicalName }) =>
      runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
        const name = normalizeLogicalName(logicalName);
        return callDataverse(token, {
          method: 'GET',
          path: `EntityDefinitions(LogicalName='${name}')`,
          query: {
            $select: 'LogicalName',
            $expand: 'ManyToManyRelationships,OneToManyRelationships,ManyToOneRelationships',
          },
        });
      })
  );

  server.registerTool(
    'dataverse_global_search',
    {
      title: 'Global Dataverse Search',
      description: 'Uses the Dataverse Search API to find records across indexable tables.',
      inputSchema: {
        search: z.string().describe('Search term'),
        entities: z.array(z.string()).optional().describe('Limit to specific logical names'),
        top: z.number().int().optional().default(10),
      },
    },
    async ({ search, entities = [], top = 10 }) =>
      runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
        if (!search) throw new Error('search string is required');
        const body = {
          search,
          top,
          ...(entities.length > 0 ? { entities: entities.map((e) => ({ name: e })) } : {}),
        };
        return callDataverse(token, {
          method: 'POST',
          path: 'search',
          body,
        });
      })
  );

  server.registerTool(
    'dataverse_create_mda',
    {
      title: 'Create Model-Driven App',
      description: 'Create a new App Module (Model-Driven App) and attach entity components.',
      inputSchema: {
        name: z.string().describe('Display name for the app'),
        uniqueName: z.string().describe('Unique internal name (e.g. prefix_myapp)'),
        description: z.string().optional(),
        entityLogicalNames: z.array(z.string()).describe('List of tables (entities) to include in the app'),
      },
    },
    async ({ name, uniqueName, description = '', entityLogicalNames = [] }) =>
      runWithDataverseToken(getInboundAccessToken, authMode, async (token) => {
        if (!name) throw new Error('name is required');

        // 1. Create the AppModule
        const appPayload = {
          name: uniqueName,
          displayname: name,
          description: description || `App created by MCP`,
          clienttype: 4, // Web
        };

        let appCreateResponse;
        try {
          appCreateResponse = await callDataverse(token, {
            method: 'POST',
            path: 'appmodules',
            body: appPayload,
            headers: { 'Prefer': 'return=representation' },
          });
        } catch (err) {
          throw new Error(`Failed to create AppModule: ${err.message}`);
        }

        const appId = appCreateResponse?.data?.appmoduleid;
        if (!appId || entityLogicalNames.length === 0) {
          return { app: appCreateResponse.data, componentsAdded: false };
        }

        // 2. Resolve ObjectTypeCodes (Entity metadata ID) for each entity
        // AddAppComponents requires the object id. For tables, it's the EntityMetadataId
        const componentsToAdd = [];

        for (const logicalName of entityLogicalNames) {
          try {
            const meta = await callDataverse(token, {
              method: 'GET',
              path: `EntityDefinitions(LogicalName='${logicalName}')`,
              query: { $select: 'MetadataId' }
            });
            if (meta?.data?.MetadataId) {
              componentsToAdd.push({
                componentType: 1, // Entity
                componentId: meta.data.MetadataId
              });
            }
          } catch (e) {
            console.warn(`Could not resolve entity ${logicalName} for MDA components.`);
          }
        }

        // 3. Add components to the app
        let addComponentsOk = false;
        if (componentsToAdd.length > 0) {
          try {
            const bodyElements = componentsToAdd.map(c => `{"@odata.type":"Microsoft.Dynamics.CRM.appcomponent", "componenttype": ${c.componentType}, "objectid": "${c.componentId}"}`);
            const rawBody = `{"Components": [${bodyElements.join(',')}]}`;

            const reqHeaders = {
              'Authorization': `Bearer ${token}`,
              'Accept': 'application/json',
              'OData-Version': '4.0',
              'OData-MaxVersion': '4.0',
              'Content-Type': 'application/json',
            };

            // Raw fetch specifically to pass nested JSON strings easily for AddAppComponents
            const res = await fetch(`${DATAVERSE_API_BASE}/appmodules(${appId})/Microsoft.Dynamics.CRM.AddAppComponents`, {
              method: 'POST',
              headers: reqHeaders,
              body: rawBody
            });

            if (res.ok) {
              addComponentsOk = true;
            }
          } catch (err) {
            console.warn(`Failed to add components to app: ${err.message}`);
          }
        }

        // 4. Publish App
        try {
          await callDataverse(token, {
            method: 'POST',
            path: 'PublishAllXml',
            body: {},
          });
        } catch (e) {
          // Ignore publish failure
        }

        return {
          app: appCreateResponse.data,
          componentsAdded: addComponentsOk,
          published: true
        };
      })
  );

  return server;
}
