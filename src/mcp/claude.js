#!/usr/bin/env node

import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import dotenv from 'dotenv';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
dotenv.config({ path: resolve(__dirname, '../../.env') });

import process from "node:process";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { buildMcpServer } from "./server.js";

async function main() {
  try {
    console.error("Starting MCP Claude server...");

    const transport = new StdioServerTransport();
    const server = buildMcpServer({ authMode: 'client_credentials' });

    await server.connect(transport);

    console.error("MCP Claude server connected");
  } catch (err) {
    console.error("Fatal MCP error:", err);
    process.exit(1);
  }
}

main();