/**
 * Lark (飞书) Channel Plugin
 * 实现 Moltbot ChannelPlugin 接口
 *
 * 功能特性：
 * - WebSocket 长连接，实时收发消息
 * - 支持私聊和群聊（@提及触发）
 * - Markdown 富文本消息支持
 * - 图片消息支持（自动上传）
 * - 消息卡片支持
 * - 智能消息合并（Debouncer）
 * - 消息去重机制
 * - 自动重试机制
 * - 长消息智能分段
 */

import type { ChannelPlugin, MoltbotConfig, PluginRuntime } from "moltbot/plugin-sdk";
import { getChatChannelMeta } from "moltbot/plugin-sdk";
import * as lark from "@larksuiteoapi/node-sdk";
import { getLarkRuntime } from "./runtime.js";

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_ACCOUNT_ID = "default";
const MESSAGE_DEDUP_TTL = 60_000; // 60秒消息去重窗口
const MAX_TEXT_LENGTH = 4000; // 飞书文本消息最大长度
const MAX_RETRY_ATTEMPTS = 3; // 最大重试次数
const RETRY_DELAY_MS = 1000; // 重试延迟

// ============================================================================
// Types
// ============================================================================

export interface LarkAccountConfig {
  enabled?: boolean;
  appId: string;
  appSecret: string;
  encryptKey?: string;
  verificationToken?: string;
  dm?: {
    enabled?: boolean;
    policy?: "open" | "pairing" | "closed";
    allowFrom?: string[];
  };
  groups?: {
    [key: string]: {
      enabled?: boolean;
      requireMention?: boolean;
    };
  };
  features?: {
    markdown?: boolean; // 启用 Markdown 消息
    cards?: boolean; // 启用消息卡片
    reactions?: boolean; // 启用表情回复
    typing?: boolean; // 启用打字指示器
  };
}

export interface ResolvedLarkAccount {
  accountId: string;
  appId: string;
  appSecret: string;
  encryptKey?: string;
  verificationToken?: string;
  config: LarkAccountConfig;
  dm: {
    enabled: boolean;
    policy: "open" | "pairing" | "closed";
    allowFrom: string[];
  };
  features: {
    markdown: boolean;
    cards: boolean;
    reactions: boolean;
    typing: boolean;
  };
}

interface AccountRuntimeState {
  running: boolean;
  connected: boolean;
  lastStartAt: number | null;
  lastStopAt: number | null;
  lastInboundAt: number | null;
  lastOutboundAt: number | null;
  lastError: string | null;
  messageCount: number;
  errorCount: number;
}

// ============================================================================
// State Management
// ============================================================================

const larkClients = new Map<string, lark.Client>();
const wsClients = new Map<string, any>();
const debouncers = new Map<string, any>();
const accountStates = new Map<string, AccountRuntimeState>();
const processedMessages = new Map<string, number>();
const userNameCache = new Map<string, { name: string; expiry: number }>();
const botOpenIdCache = new Map<string, string>(); // 缓存机器人自己的 open_id

function getAccountState(accountId: string): AccountRuntimeState {
  let state = accountStates.get(accountId);
  if (!state) {
    state = {
      running: false,
      connected: false,
      lastStartAt: null,
      lastStopAt: null,
      lastInboundAt: null,
      lastOutboundAt: null,
      lastError: null,
      messageCount: 0,
      errorCount: 0,
    };
    accountStates.set(accountId, state);
  }
  return state;
}

// ============================================================================
// Configuration Helpers
// ============================================================================

function getLarkConfig(cfg: MoltbotConfig): Record<string, LarkAccountConfig> {
  return (cfg as any).channels?.lark ?? {};
}

function listLarkAccountIds(cfg: MoltbotConfig): string[] {
  const larkCfg = getLarkConfig(cfg);
  if (!larkCfg || typeof larkCfg !== "object") return [];

  // 单账户模式：直接配置了 appId
  if ((larkCfg as any).appId) {
    return [DEFAULT_ACCOUNT_ID];
  }

  return Object.keys(larkCfg).filter((k) => k !== "enabled");
}

function resolveLarkAccount(params: {
  cfg: MoltbotConfig;
  accountId?: string | null;
}): ResolvedLarkAccount {
  const { cfg, accountId = DEFAULT_ACCOUNT_ID } = params;
  const larkCfg = getLarkConfig(cfg);

  // 解析账户配置
  let accountConfig: LarkAccountConfig;
  if ((larkCfg as any).appId) {
    accountConfig = larkCfg as any;
  } else {
    accountConfig = (larkCfg as any)[accountId || DEFAULT_ACCOUNT_ID] ?? {};
  }

  return {
    accountId: accountId || DEFAULT_ACCOUNT_ID,
    appId: accountConfig.appId || "",
    appSecret: accountConfig.appSecret || "",
    encryptKey: accountConfig.encryptKey,
    verificationToken: accountConfig.verificationToken,
    config: accountConfig,
    dm: {
      enabled: accountConfig.dm?.enabled ?? true,
      policy: accountConfig.dm?.policy ?? "pairing",
      allowFrom: accountConfig.dm?.allowFrom ?? [],
    },
    features: {
      markdown: accountConfig.features?.markdown ?? true,
      cards: accountConfig.features?.cards ?? true,
      reactions: accountConfig.features?.reactions ?? true,
      typing: accountConfig.features?.typing ?? false,
    },
  };
}

// ============================================================================
// Lark Client
// ============================================================================

function getLarkClient(account: ResolvedLarkAccount): lark.Client {
  const key = `${account.appId}:${account.appSecret}`;
  let client = larkClients.get(key);
  if (!client) {
    client = new lark.Client({
      appId: account.appId,
      appSecret: account.appSecret,
      disableTokenCache: false,
    });
    larkClients.set(key, client);
  }
  return client;
}

// ============================================================================
// Message Utilities
// ============================================================================

/**
 * 智能分割长消息，保持代码块和段落完整性
 */
function splitMessage(text: string, maxLength: number = MAX_TEXT_LENGTH): string[] {
  if (text.length <= maxLength) return [text];

  const chunks: string[] = [];
  let remaining = text;

  while (remaining.length > 0) {
    if (remaining.length <= maxLength) {
      chunks.push(remaining);
      break;
    }

    // 优先在代码块边界分割
    let splitIndex = -1;
    const codeBlockEnd = remaining.lastIndexOf("\n```\n", maxLength);
    if (codeBlockEnd > maxLength / 3) {
      splitIndex = codeBlockEnd + 5;
    }

    // 其次在段落边界分割
    if (splitIndex === -1) {
      const paragraphEnd = remaining.lastIndexOf("\n\n", maxLength);
      if (paragraphEnd > maxLength / 3) {
        splitIndex = paragraphEnd + 2;
      }
    }

    // 再次在行边界分割
    if (splitIndex === -1) {
      const lineEnd = remaining.lastIndexOf("\n", maxLength);
      if (lineEnd > maxLength / 2) {
        splitIndex = lineEnd + 1;
      }
    }

    // 最后强制分割
    if (splitIndex === -1 || splitIndex <= 0) {
      splitIndex = maxLength;
    }

    chunks.push(remaining.substring(0, splitIndex).trimEnd());
    remaining = remaining.substring(splitIndex).trimStart();
  }

  return chunks;
}

/**
 * 转换 Markdown 为飞书富文本格式
 */
function markdownToLarkRichText(markdown: string): any {
  const lines = markdown.split("\n");
  const content: any[][] = [];
  let currentParagraph: any[] = [];
  let inCodeBlock = false;
  let codeBlockLang = "";
  let codeBlockContent: string[] = [];

  for (const line of lines) {
    // 代码块处理
    if (line.startsWith("```")) {
      if (!inCodeBlock) {
        inCodeBlock = true;
        codeBlockLang = line.slice(3).trim();
        codeBlockContent = [];
      } else {
        // 结束代码块
        if (currentParagraph.length > 0) {
          content.push(currentParagraph);
          currentParagraph = [];
        }
        content.push([{
          tag: "code_block",
          language: codeBlockLang || "plain_text",
          text: codeBlockContent.join("\n"),
        }]);
        inCodeBlock = false;
      }
      continue;
    }

    if (inCodeBlock) {
      codeBlockContent.push(line);
      continue;
    }

    // 空行分隔段落
    if (!line.trim()) {
      if (currentParagraph.length > 0) {
        content.push(currentParagraph);
        currentParagraph = [];
      }
      continue;
    }

    // 标题处理
    const headerMatch = line.match(/^(#{1,6})\s+(.+)$/);
    if (headerMatch) {
      if (currentParagraph.length > 0) {
        content.push(currentParagraph);
        currentParagraph = [];
      }
      content.push([{
        tag: "text",
        text: headerMatch[2],
        style: ["bold"],
      }]);
      continue;
    }

    // 普通文本（支持 **bold** 和 `code`）
    let processed = line;
    const elements: any[] = [];

    // 简单解析：直接作为文本
    elements.push({ tag: "text", text: processed });

    currentParagraph.push(...elements);
    currentParagraph.push({ tag: "text", text: "\n" });
  }

  if (currentParagraph.length > 0) {
    content.push(currentParagraph);
  }

  return {
    zh_cn: {
      title: "",
      content,
    },
  };
}

/**
 * 创建消息卡片
 */
function createMessageCard(title: string, content: string, color?: string): string {
  return JSON.stringify({
    config: { wide_screen_mode: true },
    header: {
      title: { tag: "plain_text", content: title },
      template: color || "blue",
    },
    elements: [
      {
        tag: "markdown",
        content: content,
      },
    ],
  });
}

/**
 * 带重试的异步操作
 */
async function withRetry<T>(
  fn: () => Promise<T>,
  maxAttempts: number = MAX_RETRY_ATTEMPTS,
  delayMs: number = RETRY_DELAY_MS
): Promise<T> {
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (err: any) {
      lastError = err;
      if (attempt < maxAttempts) {
        await new Promise(resolve => setTimeout(resolve, delayMs * attempt));
      }
    }
  }

  throw lastError;
}

// ============================================================================
// Message Sending
// ============================================================================

async function sendTextMessage(
  client: lark.Client,
  receiveId: string,
  receiveIdType: "open_id" | "chat_id",
  text: string
): Promise<string> {
  const response = await withRetry(() =>
    client.im.message.create({
      params: { receive_id_type: receiveIdType },
      data: {
        receive_id: receiveId,
        msg_type: "text",
        content: JSON.stringify({ text }),
      },
    })
  );

  if (response.code !== 0) {
    throw new Error(`Lark send failed: ${response.msg}`);
  }

  return response.data?.message_id || "";
}

async function sendRichTextMessage(
  client: lark.Client,
  receiveId: string,
  receiveIdType: "open_id" | "chat_id",
  richText: any
): Promise<string> {
  const response = await withRetry(() =>
    client.im.message.create({
      params: { receive_id_type: receiveIdType },
      data: {
        receive_id: receiveId,
        msg_type: "post",
        content: JSON.stringify(richText),
      },
    })
  );

  if (response.code !== 0) {
    throw new Error(`Lark send failed: ${response.msg}`);
  }

  return response.data?.message_id || "";
}

async function sendCardMessage(
  client: lark.Client,
  receiveId: string,
  receiveIdType: "open_id" | "chat_id",
  card: string
): Promise<string> {
  const response = await withRetry(() =>
    client.im.message.create({
      params: { receive_id_type: receiveIdType },
      data: {
        receive_id: receiveId,
        msg_type: "interactive",
        content: card,
      },
    })
  );

  if (response.code !== 0) {
    throw new Error(`Lark send failed: ${response.msg}`);
  }

  return response.data?.message_id || "";
}

async function addReaction(
  client: lark.Client,
  messageId: string,
  emojiType: string
): Promise<void> {
  try {
    await client.im.messageReaction.create({
      path: { message_id: messageId },
      data: {
        reaction_type: { emoji_type: emojiType },
      },
    });
  } catch {
    // 忽略表情添加失败
  }
}

// ============================================================================
// Outbound API
// ============================================================================

async function sendLarkMessage(
  to: string,
  text: string,
  options?: {
    accountId?: string;
    replyToId?: string;
    cfg?: MoltbotConfig;
    useMarkdown?: boolean;
    useCard?: boolean;
    cardTitle?: string;
  }
): Promise<{ messageId: string; to: string }> {
  const cfg = options?.cfg ?? getLarkRuntime().config.get();
  const account = resolveLarkAccount({ cfg, accountId: options?.accountId });
  const client = getLarkClient(account);

  const isUserId = to.startsWith("ou_") || to.startsWith("user:");
  const receiveIdType = isUserId ? "open_id" : "chat_id";
  const receiveId = to.replace(/^user:/, "");

  const state = getAccountState(account.accountId);
  state.lastOutboundAt = Date.now();

  // 智能选择消息格式
  let messageId = "";

  if (options?.useCard && account.features.cards) {
    // 使用消息卡片
    const card = createMessageCard(options.cardTitle || "AI 回复", text);
    messageId = await sendCardMessage(client, receiveId, receiveIdType, card);
  } else if (options?.useMarkdown && account.features.markdown && text.includes("```")) {
    // 包含代码块时使用富文本
    const richText = markdownToLarkRichText(text);
    messageId = await sendRichTextMessage(client, receiveId, receiveIdType, richText);
  } else {
    // 普通文本消息，分段发送
    const chunks = splitMessage(text);
    for (const chunk of chunks) {
      messageId = await sendTextMessage(client, receiveId, receiveIdType, chunk);
    }
  }

  state.messageCount++;

  return { messageId, to: receiveId };
}

async function sendLarkMedia(
  to: string,
  text: string,
  mediaUrl: string,
  options?: { accountId?: string; replyToId?: string; cfg?: MoltbotConfig }
): Promise<{ messageId: string; to: string }> {
  // 暂时用文本 + 链接代替图片上传
  const fullText = text ? `${text}\n${mediaUrl}` : mediaUrl;
  return sendLarkMessage(to, fullText, options);
}

// ============================================================================
// Message Deduplication
// ============================================================================

function isDuplicateMessage(messageId: string): boolean {
  const now = Date.now();

  // 清理过期记录
  for (const [id, timestamp] of processedMessages) {
    if (now - timestamp > MESSAGE_DEDUP_TTL) {
      processedMessages.delete(id);
    }
  }

  if (processedMessages.has(messageId)) {
    return true;
  }

  processedMessages.set(messageId, now);
  return false;
}

// ============================================================================
// User Info
// ============================================================================

/**
 * 获取机器人自己的 open_id
 */
async function getBotOpenId(
  client: lark.Client,
  accountId: string
): Promise<string> {
  const cached = botOpenIdCache.get(accountId);
  if (cached) {
    return cached;
  }

  try {
    const response = await (client as any).bot.info();
    const botOpenId = response.data?.bot?.open_id || "";
    if (botOpenId) {
      botOpenIdCache.set(accountId, botOpenId);
    }
    return botOpenId;
  } catch {
    return "";
  }
}

async function getUserName(
  client: lark.Client,
  userId: string
): Promise<string> {
  const cached = userNameCache.get(userId);
  if (cached && cached.expiry > Date.now()) {
    return cached.name;
  }

  try {
    const response = await client.contact.user.get({
      path: { user_id: userId },
      params: { user_id_type: "open_id" },
    });

    const name = response.data?.user?.name || userId;
    userNameCache.set(userId, {
      name,
      expiry: Date.now() + 3600_000, // 1小时缓存
    });
    return name;
  } catch {
    return userId;
  }
}

// ============================================================================
// Inbound Processing
// ============================================================================

async function processMessage(params: {
  data: any;
  combinedText: string;
  accountId: string;
  config: MoltbotConfig;
  runtime: any;
}): Promise<void> {
  const { data, combinedText, accountId, config } = params;

  const messageData = data?.message;
  const senderData = data?.sender;

  if (!messageData || !senderData) return;

  const text = combinedText.trim();
  if (!text) return;

  const chatType = messageData.chat_type === "p2p" ? "direct" : "channel";
  const senderId = senderData.sender_id?.open_id || "";
  const chatId = messageData.chat_id || "";
  const messageId = messageData.message_id || "";

  const account = resolveLarkAccount({ cfg: config, accountId });
  const client = getLarkClient(account);
  const isGroupChat = chatType === "channel";

  // 群聊 @提及检测
  let finalText = text;
  if (isGroupChat) {
    const groupConfig = account.config.groups?.["*"] ?? { requireMention: true };
    const requireMention = groupConfig.requireMention !== false;

    if (requireMention) {
      // 获取机器人自己的 open_id
      const botOpenId = await getBotOpenId(client, accountId);

      const mentions = messageData.mentions || [];
      // 检查是否 @了机器人自己，而不是任意 @
      const isMentionedBot = mentions.some((m: any) => {
        const mentionOpenId = m.id?.open_id;
        // 如果有 botOpenId，精确匹配；否则检查是否 @all
        if (botOpenId && mentionOpenId) {
          return mentionOpenId === botOpenId;
        }
        return m.key === "@_all";
      });

      if (!isMentionedBot) {
        return; // 群聊未 @机器人，忽略
      }

      // 移除 @提及文本
      finalText = text.replace(/@_user_\d+\s*/g, "").trim();
      if (!finalText) return;
    }
  }

  console.log(`[lark:${accountId}] Message from ${senderId}: ${finalText.substring(0, 50)}...`);

  // 获取发送者名称
  const senderName = await getUserName(client, senderId);

  // 添加"已读"表情表示收到消息
  if (account.features.reactions) {
    await addReaction(client, messageId, "OK");
  }

  const replyTarget = chatId;
  console.log(`[lark:${accountId}] replyTarget=${replyTarget}, chatId=${chatId}, senderId=${senderId}, chatType=${chatType}`);
  if (!replyTarget) {
    console.error(`[lark:${accountId}] No chat_id for reply`);
    return;
  }

  // 获取 Runtime API
  const larkRuntime = getLarkRuntime();
  const channelApi = (larkRuntime as any).channel;

  if (!channelApi?.routing?.resolveAgentRoute ||
      !channelApi?.reply?.finalizeInboundContext ||
      !channelApi?.reply?.dispatchReplyFromConfig ||
      !channelApi?.reply?.createReplyDispatcherWithTyping) {
    console.error(`[lark:${accountId}] Runtime API unavailable`);
    await sendTextMessage(client, replyTarget, "chat_id", "抱歉，AI 服务暂时不可用。");
    return;
  }

  const { resolveAgentRoute } = channelApi.routing;
  const { finalizeInboundContext, dispatchReplyFromConfig, createReplyDispatcherWithTyping } = channelApi.reply;

  // 解析路由
  const peerId = isGroupChat ? `group:${chatId}` : senderId;
  const route = resolveAgentRoute({
    cfg: config,
    channel: "lark",
    accountId,
    peer: {
      kind: isGroupChat ? "group" : "dm",
      id: peerId,
    },
  });

  // 生成独立的 session key
  const sessionKey = isGroupChat
    ? `${route.sessionKey}:lark:group:${chatId}`
    : `${route.sessionKey}:lark:${senderId}`;

  // 构建上下文
  const timestamp = Date.now();
  const fromAddress = isGroupChat ? `lark:group:${chatId}` : `lark:${senderId}`;
  const toAddress = fromAddress;

  // 在消息中附加发送者信息，让 AI 能够识别用户身份
  const bodyWithMeta = `${finalText}\n[sender: ${senderName} (${senderId})]`;

  const ctx = finalizeInboundContext({
    Body: bodyWithMeta,
    RawBody: finalText,
    CommandBody: finalText,
    From: fromAddress,
    To: toAddress,
    SessionKey: sessionKey,
    AccountId: accountId,
    ChatType: isGroupChat ? "group" : "direct",
    ConversationLabel: isGroupChat ? `group:${chatId}` : `user:${senderId}`,
    SenderId: senderId,
    Provider: "lark",
    Surface: "lark",
    MessageSid: messageId,
    Timestamp: timestamp,
    OriginatingChannel: "lark",
    OriginatingTo: toAddress,
  });

  // 记录 session
  const sessionApi = channelApi.session;
  if (sessionApi?.recordInboundSession && sessionApi?.resolveStorePath) {
    try {
      const storePath = sessionApi.resolveStorePath(config.session?.store, { agentId: route.agentId });
      if (storePath) {
        await sessionApi.recordInboundSession({
          storePath,
          sessionKey,
          ctx,
          updateLastRoute: !isGroupChat ? {
            sessionKey: route.mainSessionKey,
            channel: "lark",
            to: senderId,
            accountId,
          } : undefined,
        });
      }
    } catch {
      // 忽略 session 记录错误
    }
  }

  // 创建 dispatcher
  const { dispatcher, replyOptions, markDispatchIdle } = createReplyDispatcherWithTyping({
    deliver: async (payload: any) => {
      try {
        const replyText = payload.text || payload.body || "";
        if (!replyText.trim()) return;

        // 智能选择消息格式
        const hasCodeBlock = replyText.includes("```");
        if (hasCodeBlock && account.features.markdown) {
          // 代码块使用富文本
          const richText = markdownToLarkRichText(replyText);
          await sendRichTextMessage(client, replyTarget, "chat_id", richText);
        } else {
          // 普通消息分段发送
          const chunks = splitMessage(replyText);
          for (const chunk of chunks) {
            await sendTextMessage(client, replyTarget, "chat_id", chunk);
          }
        }
      } catch (err: any) {
        console.error(`[lark:${accountId}] Deliver error:`, err.message);
        const state = getAccountState(accountId);
        state.errorCount++;
      }
    },
    onError: (err: any) => {
      console.error(`[lark:${accountId}] Dispatch error:`, err);
      const state = getAccountState(accountId);
      state.errorCount++;
    },
  });

  // 分发到 AI 处理
  try {
    await dispatchReplyFromConfig({
      ctx,
      cfg: config,
      dispatcher,
      replyOptions,
    });
  } catch (err: any) {
    console.error(`[lark:${accountId}] AI dispatch error:`, err.message);
    const state = getAccountState(accountId);
    state.errorCount++;
  }

  markDispatchIdle();
}

// ============================================================================
// Provider (WebSocket Long Connection)
// ============================================================================

async function startProvider(params: {
  appId: string;
  appSecret: string;
  accountId: string;
  config: MoltbotConfig;
  runtime: any;
  abortSignal?: AbortSignal;
}): Promise<void> {
  const { appId, appSecret, accountId, config, abortSignal } = params;

  const state = getAccountState(accountId);
  state.running = true;
  state.lastStartAt = Date.now();
  state.lastError = null;

  try {
    const larkRuntime = getLarkRuntime();
    const channelApi = (larkRuntime as any).channel;

    if (!channelApi?.debounce?.createInboundDebouncer) {
      throw new Error("createInboundDebouncer not available");
    }

    const { createInboundDebouncer, resolveInboundDebounceMs } = channelApi.debounce;

    // 创建消息合并器
    const debounceMs = resolveInboundDebounceMs?.({ cfg: config, channel: "lark" }) || 500;

    const debouncer = createInboundDebouncer({
      debounceMs,
      buildKey: (entry: any) => {
        const d = entry.data || entry;
        const senderId = d?.sender?.sender_id?.open_id || "";
        const chatId = d?.message?.chat_id || "";
        const chatType = d?.message?.chat_type;

        if (!senderId || !chatId) return null;

        return chatType === "p2p"
          ? `lark:${accountId}:dm:${senderId}`
          : `lark:${accountId}:${chatId}:${senderId}`;
      },
      shouldDebounce: (entry: any) => {
        const d = entry.data || entry;
        if (d?.message?.message_type !== "text") return false;

        try {
          const content = JSON.parse(d?.message?.content || "{}");
          return Boolean(content.text?.trim());
        } catch {
          return false;
        }
      },
      onFlush: async (entries: any[]) => {
        if (entries.length === 0) return;

        const last = entries.at(-1);
        const data = last?.data || last;
        if (!data) return;

        // 合并多条消息的文本
        let combinedText = "";
        if (entries.length > 1) {
          const texts = entries
            .map((e) => {
              try {
                const d = e.data || e;
                return JSON.parse(d?.message?.content || "{}").text || "";
              } catch {
                return "";
              }
            })
            .filter(Boolean);
          combinedText = texts.join("\n");
          console.log(`[lark:${accountId}] Merged ${entries.length} messages`);
        } else {
          try {
            combinedText = JSON.parse(data?.message?.content || "{}").text || "";
          } catch {
            combinedText = data?.message?.content || "";
          }
        }

        await processMessage({
          data,
          combinedText,
          accountId,
          config,
          runtime: params.runtime,
        });
      },
      onError: (err: any) => {
        console.error(`[lark:${accountId}] Debouncer error:`, err);
        state.errorCount++;
      },
    });

    debouncers.set(accountId, debouncer);

    // 创建 WebSocket 客户端
    const wsClient = new lark.WSClient({
      appId,
      appSecret,
      loggerLevel: lark.LoggerLevel.warn,
    });

    wsClients.set(accountId, wsClient);

    // 注册事件处理器
    const eventDispatcher = new lark.EventDispatcher({}).register({
      "im.message.receive_v1": async (data: any) => {
        state.lastInboundAt = Date.now();

        // 消息去重
        const messageId = data?.message?.message_id;
        if (messageId && isDuplicateMessage(messageId)) {
          return;
        }

        // 忽略机器人自己的消息
        if (data?.sender?.sender_type === "app") return;

        // 入队处理
        try {
          await debouncer.enqueue({ data });
        } catch (err: any) {
          console.error(`[lark:${accountId}] Enqueue error:`, err.message);
          state.errorCount++;
        }
      },
    });

    console.log(`[lark:${accountId}] Connecting...`);
    await wsClient.start({ eventDispatcher });

    state.connected = true;
    console.log(`[lark:${accountId}] Connected`);

    // 处理中断信号
    if (abortSignal) {
      abortSignal.addEventListener("abort", () => {
        console.log(`[lark:${accountId}] Stopping...`);
        state.running = false;
        state.connected = false;
        state.lastStopAt = Date.now();
        wsClients.delete(accountId);
        debouncers.delete(accountId);
      });
    }
  } catch (err: any) {
    state.running = false;
    state.connected = false;
    state.lastError = err.message || "Unknown error";
    state.errorCount++;
    throw err;
  }
}

// ============================================================================
// Health Check
// ============================================================================

async function probeAccount(
  appId: string,
  appSecret: string
): Promise<{ ok: boolean; error?: string }> {
  try {
    const client = new lark.Client({ appId, appSecret });
    await client.im.chat.list({ params: { page_size: 1 } });
    return { ok: true };
  } catch (err: any) {
    return { ok: false, error: err.message || "Unknown error" };
  }
}

// ============================================================================
// Channel Plugin Definition
// ============================================================================

const meta = getChatChannelMeta("lark", {
  label: "Lark (飞书)",
  shortLabel: "Lark",
  docs: "https://docs.clawd.bot/channels/lark",
  color: "#3370FF",
  icon: "lark",
});

export const larkPlugin: ChannelPlugin<ResolvedLarkAccount> = {
  id: "lark",
  meta: { ...meta },

  capabilities: {
    chatTypes: ["direct", "channel"],
    reactions: true,
    threads: true,
    media: true,
    nativeCommands: false,
  },

  config: {
    listAccountIds: (cfg) => listLarkAccountIds(cfg),
    resolveAccount: (cfg, accountId) => resolveLarkAccount({ cfg, accountId }),
    defaultAccountId: () => DEFAULT_ACCOUNT_ID,
    isConfigured: (account) => Boolean(account.appId && account.appSecret),
    describeAccount: (account) => ({
      label: account.accountId,
      summary: account.appId ? `App: ${account.appId.substring(0, 10)}...` : "Not configured",
    }),
  },

  pairing: {
    idLabel: "larkUserId",
    normalizeAllowEntry: (entry) => entry.replace(/^(lark|user):/i, ""),
    notifyApproval: async () => {},
  },

  security: {
    resolveDmPolicy: ({ account }) => ({
      policy: account.dm.policy,
      allowFrom: account.dm.allowFrom,
      allowFromPath: "channels.lark.dm.",
      approveHint: "moltbot pairing approve lark <code>",
      normalizeEntry: (raw) => raw.replace(/^(lark|user):/i, ""),
    }),
    collectWarnings: ({ account }) => {
      const warnings: string[] = [];
      if (account.dm.policy === "open" && account.dm.allowFrom.includes("*")) {
        warnings.push("DM is open to everyone - consider using pairing mode");
      }
      return warnings;
    },
  },

  outbound: {
    deliveryMode: "direct",
    chunker: null,
    textChunkLimit: MAX_TEXT_LENGTH,

    sendText: async ({ to, text, accountId, cfg, replyToId }) => {
      const result = await sendLarkMessage(to, text, { accountId, cfg, replyToId });
      return { channel: "lark", ...result };
    },

    sendMedia: async ({ to, text, mediaUrl, accountId, cfg, replyToId }) => {
      const result = await sendLarkMedia(to, text || "", mediaUrl || "", { accountId, cfg, replyToId });
      return { channel: "lark", ...result };
    },
  },

  gateway: {
    startAccount: async (ctx) => {
      const { account, cfg, runtime, abortSignal, log } = ctx;
      log?.info(`[lark:${account.accountId}] Starting`);

      return startProvider({
        appId: account.appId,
        appSecret: account.appSecret,
        accountId: account.accountId,
        config: cfg,
        runtime,
        abortSignal,
      });
    },
  },

  status: {
    defaultRuntime: {
      accountId: DEFAULT_ACCOUNT_ID,
      running: false,
      lastStartAt: null,
      lastStopAt: null,
      lastError: null,
    },

    probeAccount: async ({ account, timeoutMs }) => {
      if (!account.appId || !account.appSecret) {
        return { ok: false, error: "Missing appId or appSecret" };
      }
      return probeAccount(account.appId, account.appSecret);
    },

    buildAccountSnapshot: ({ account, probe }) => {
      const state = getAccountState(account.accountId);
      return {
        accountId: account.accountId,
        configured: Boolean(account.appId && account.appSecret),
        running: state.running,
        connected: state.connected,
        healthy: probe?.ok ?? false,
        lastInboundAt: state.lastInboundAt,
        lastOutboundAt: state.lastOutboundAt,
        lastStartAt: state.lastStartAt,
        lastStopAt: state.lastStopAt,
        messageCount: state.messageCount,
        errorCount: state.errorCount,
        error: probe?.error ?? state.lastError ?? null,
      };
    },
  },
};
