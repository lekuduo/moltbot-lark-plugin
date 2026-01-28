# Moltbot Lark Plugin

飞书 (Lark) 频道插件，让你的 AI 助手接入飞书。

[English](#english) | [中文](#中文)

## 效果展示 / Screenshots

<p align="center">
  <img src="screenshots/demo1.png" width="400" alt="群聊效果">
  <img src="screenshots/demo2.png" width="400" alt="对话总结">
</p>

---

## 中文

### 功能

- WebSocket 长连接，实时收发消息
- 私聊 + 群聊（@提及触发）
- 自动重试、消息去重、长消息分段
- 用户身份识别（ID + 名称）

### 快速开始

**1. 安装插件**

```bash
cd ~/.moltbot/extensions
git clone https://github.com/lekuduo/moltbot-lark-plugin.git lark
cd lark && npm install
```

**2. 配置飞书应用**

在[飞书开发者后台](https://open.feishu.cn/)：
- 创建企业应用，获取 App ID 和 App Secret
- 权限：`im:message` `im:message.receive_v1`
- 事件订阅：启用长连接，添加 `im.message.receive_v1`

**3. 配置 Moltbot**

编辑 `~/.moltbot/moltbot.json`：

```json
{
  "channels": {
    "lark": {
      "enabled": true,
      "appId": "cli_xxxxxxxxxx",
      "appSecret": "xxxxxxxxxxxxxxxx",
      "dm": { "enabled": true, "policy": "open", "allowFrom": ["*"] },
      "groups": { "*": { "requireMention": true } }
    }
  }
}
```

**4. 重启 Gateway**

```bash
moltbot gateway stop && moltbot gateway
```

### 配置说明

| 配置项 | 说明 |
|--------|------|
| `appId` / `appSecret` | 飞书应用凭据 |
| `dm.policy` | 私聊策略：`open`（开放）/ `pairing`（配对）/ `closed`（关闭）|
| `dm.allowFrom` | 允许私聊的用户 ID 列表，`["*"]` 表示所有人 |
| `groups.*.requireMention` | 群聊是否需要 @机器人 |

### 高级配置

#### CLI 模式 vs API 模式

**CLI 模式问题**：使用 `claude-cli/*` 时可能遇到 "Session ID already in use" 错误。

原因：Claude CLI 有账户级别的 session 锁机制，如果终端已运行 Claude CLI，Gateway 复用相同 session ID 会冲突。

解决方案：
- **方案 A（推荐）**：改用 API 模式，配置第三方代理
- **方案 B**：确保终端没有运行其他 Claude CLI 实例

#### 使用第三方 API 代理

如果使用 Claude API 代理服务，在 `moltbot.json` 中配置：

```json
{
  "model": "your-provider/model-name",
  "modelConfig": {
    "your-provider": {
      "type": "anthropic",
      "baseUrl": "https://your-proxy-api.com/api",
      "apiKey": "your-api-key"
    }
  }
}
```

#### 禁用工具调用

某些 API 代理可能不完全支持工具调用，导致 `server_tool_use` 错误。这是因为代理检测到 system prompt 中的工具定义后在服务端处理，返回格式不兼容。

添加以下配置禁用问题工具：

```json
{
  "tools": {
    "deny": ["web_search", "web_fetch"]
  }
}
```

### 故障排除

| 问题 | 解决 |
|------|------|
| 连接失败 | 检查 App ID/Secret，确认长连接已启用 |
| 收不到消息 | 确认已订阅 `im.message.receive_v1` 事件 |
| Session ID already in use | 关闭终端中的 Claude CLI，或改用 API 模式 |
| `server_tool_use` 错误 | 在 `tools.deny` 中禁用 `web_search` 和 `web_fetch` |
| 查看日志 | `tail -f ~/.moltbot/gateway.log` |

---

<a name="english"></a>
## English

### Features

- WebSocket long connection for real-time messaging
- DM + Group chat (with @mention trigger)
- Auto-retry, deduplication, long message splitting
- User identity (ID + name)

### Quick Start

**1. Install**

```bash
cd ~/.moltbot/extensions
git clone https://github.com/lekuduo/moltbot-lark-plugin.git lark
cd lark && npm install
```

**2. Configure Lark App**

In [Lark Developer Console](https://open.feishu.cn/):
- Create enterprise app, get App ID and App Secret
- Permissions: `im:message` `im:message.receive_v1`
- Events: Enable long connection, add `im.message.receive_v1`

**3. Configure Moltbot**

Edit `~/.moltbot/moltbot.json`:

```json
{
  "channels": {
    "lark": {
      "enabled": true,
      "appId": "cli_xxxxxxxxxx",
      "appSecret": "xxxxxxxxxxxxxxxx",
      "dm": { "enabled": true, "policy": "open", "allowFrom": ["*"] },
      "groups": { "*": { "requireMention": true } }
    }
  }
}
```

**4. Restart Gateway**

```bash
moltbot gateway stop && moltbot gateway
```

### Configuration

| Option | Description |
|--------|-------------|
| `appId` / `appSecret` | Lark app credentials |
| `dm.policy` | DM policy: `open` / `pairing` / `closed` |
| `dm.allowFrom` | Allowed user IDs, `["*"]` for all |
| `groups.*.requireMention` | Require @mention in groups |

### Advanced Configuration

#### CLI Mode vs API Mode

**CLI Mode Issue**: Using `claude-cli/*` may cause "Session ID already in use" error.

Reason: Claude CLI has account-level session locking. If Claude CLI is already running in terminal, Gateway reusing the same session ID will conflict.

Solutions:
- **Option A (Recommended)**: Switch to API mode with third-party proxy
- **Option B**: Ensure no other Claude CLI instances are running

#### Using Third-party API Proxy

If using a Claude API proxy service, configure in `moltbot.json`:

```json
{
  "model": "your-provider/model-name",
  "modelConfig": {
    "your-provider": {
      "type": "anthropic",
      "baseUrl": "https://your-proxy-api.com/api",
      "apiKey": "your-api-key"
    }
  }
}
```

#### Disable Tool Calling

Some API proxies may not fully support tool calling, causing `server_tool_use` errors. This happens when the proxy detects tool definitions in system prompt and handles them server-side, returning incompatible format.

Add this config to disable problematic tools:

```json
{
  "tools": {
    "deny": ["web_search", "web_fetch"]
  }
}
```

### Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection failed | Check App ID/Secret, ensure long connection enabled |
| No messages | Verify `im.message.receive_v1` event subscription |
| Session ID already in use | Close Claude CLI in terminal, or switch to API mode |
| `server_tool_use` error | Disable `web_search` and `web_fetch` in `tools.deny` |
| View logs | `tail -f ~/.moltbot/gateway.log` |

---

## License

MIT
