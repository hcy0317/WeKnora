# Claw Skill

Claw Skill 是把 WeKnora 挂给 AI Agent 用的一种方式：安装之后，OpenClaw 生态里的 Agent 就能通过 WeKnora 的 REST API 往知识库里写内容、跨库检索。

Skill 托管在 ClawHub，包名 [`@lyingbug/weknora`](https://clawhub.ai/lyingbug/weknora)（MIT-0）。它是一层薄封装，实际能力就是 WeKnora 的 REST 接口。

## 能做什么

| 能力 | 对应接口 |
| --- | --- |
| 上传文件 | 把 PDF / Word / Excel 等文档送进知识库并自动解析向量化 |
| 导入网页 | 按 URL 抓取正文写入知识库，支持轮询解析状态 |
| 写入 Markdown | 以 Markdown 创建或编辑知识条目，适合会议记录、结构化笔记 |
| 混合检索 | 单库 `hybrid-search` 与跨库 `knowledge-search`，向量 + 关键词召回 |
| 浏览知识库 | 列出知识库与条目、查看详情 |

## 怎么配

在「设置 → 发布集成 → Claw Skill」查看安装指引，复制当前实例的 API 地址、环境变量示例和安装命令。

1. **获取 API 凭证**：在「设置 → 发布集成 → API 集成」复制 API Key 与 API 地址（引导页的「打开 API 信息」按钮会直接跳转到这里）；
2. **设置环境变量**：在终端或 `~/.zshrc` / `~/.bashrc` 里设置

   ```bash
   export WEKNORA_BASE_URL=https://your-weknora.example.com/api/v1
   export WEKNORA_API_KEY=sk-xxxxx
   ```

   `WEKNORA_BASE_URL` 需要包含 `/api/v1`，引导页的示例已填入当前实例地址。

3. **安装 Skill**：在装好 OpenClaw CLI 的环境里执行下面的命令，或到 ClawHub 页面按指引安装；

   ```bash
   openclaw skills install @lyingbug/weknora
   ```

4. **验证**：让 Agent 列一次知识库或跑一次检索，确认凭证与网络可达。

## 和 MCP 的关系

两者都是「把 WeKnora 给外部 Agent 用」，选哪个取决于对方生态：

| | Claw Skill | MCP Server（内置） |
| --- | --- | --- |
| 面向 | OpenClaw / ClawHub 生态的 Agent | 支持 MCP 协议的客户端（Claude Desktop、Cursor、Claude Code、VS Code Copilot 等） |
| 安装 | ClawHub 安装 Skill | 无需额外部署：在「设置 → 发布集成 → MCP Server」创建端点，客户端连接 `/mcp/<endpoint_id>` |
| 认证 | 空间 API Key（`WEKNORA_API_KEY`） | 每个端点独立令牌，可轮换、停用 |
| 传输 | 直接调 REST | Streamable HTTP；仅支持 stdio 的客户端通过 `mcp-remote` 桥接 |
| 能力范围 | 导入、检索、浏览（5 类） | 按端点勾选：检索与阅读、问答（端点默认 Agent）、Wiki、写入；可限定知识库范围 |
| 文档 | 本篇 | [MCP 集成](../03-features/08-mcp.md) |

需要问答、Wiki 工具，或希望按端点限定工具和知识库范围时，使用 MCP Server；在 OpenClaw 生态中导入、检索和浏览资料可使用 Claw Skill。仓库 `mcp-server/` 下的 Python MCP 服务已弃用，新接入请使用内置 MCP Server。

## 相关

- 凭证与能力收窄：[租户、用户与认证授权](../03-features/01-tenant-auth.md)
- 底层接口：[API 总览](../04-api/01-api-overview.md)
- 其它集成方式：[Chrome 插件](06-chrome-extension.md)、[MCP 集成](../03-features/08-mcp.md)
