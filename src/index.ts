import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { buildEnvelope } from "./core/audit/envelope.js";
import { indexSystemTools } from "./core/bootstrap_tools.js";

import { RetrieveTool, RetrieveInputSchema } from "./tools/retrieve.js";
import { CommitIndexTool, CommitIndexInputSchema } from "./tools/commit_index.js";
import { LogIndexTool, LogIndexInputSchema } from "./tools/log_index.js";
import { DiffIndexTool, DiffIndexInputSchema } from "./tools/diff_index.js";
import { ValidateSchemaTool, ValidateSchemaInputSchema } from "./tools/validate_schema.js";
import { CheckSupportTool, CheckSupportInputSchema } from "./tools/check_support.js";
import { RedactTool, RedactInputSchema } from "./tools/redact.js";
import { CreateTaskTool, CreateTaskInputSchema } from "./tools/create_task.js";

indexSystemTools();

const server = new McpServer({ name: "local-mcp-server", version: "1.0.0" });

function register(toolDef: any, schema: any) {
  server.tool(toolDef.name, schema.shape, async (args: any) => {
    try {
      const envelope = await toolDef.execute(args);
      return { content: [{ type: "text", text: JSON.stringify(envelope, null, 2) }] };
    } catch (err: any) {
      const errorEnvelope = buildEnvelope({
        request_id: args?.request_id,
        tool_name: toolDef.name,
        tool_version: toolDef.version,
        input: args,
        result: null,
        errors: [{
          code: "ERR_TOOL_FAILURE",
          message: err?.message ?? "Unknown error"
        }]
      });
      return { content: [{ type: "text", text: JSON.stringify(errorEnvelope, null, 2) }] };
    }
  });
}

register(RetrieveTool, RetrieveInputSchema);
register(CommitIndexTool, CommitIndexInputSchema);
register(LogIndexTool, LogIndexInputSchema);
register(DiffIndexTool, DiffIndexInputSchema);
register(ValidateSchemaTool, ValidateSchemaInputSchema);
register(CheckSupportTool, CheckSupportInputSchema);
register(RedactTool, RedactInputSchema);
register(CreateTaskTool, CreateTaskInputSchema);

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Local MCP Server running on stdio.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
