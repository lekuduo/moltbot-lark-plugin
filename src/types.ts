/**
 * Lark Channel Plugin Types
 */

export interface LarkIncomingMessage {
  messageId: string;
  chatId: string;
  chatType: "direct" | "channel";
  senderId: string;
  text: string;
  contentType: "text" | "image" | "audio" | "video" | "file" | "interactive";
  mentionedBot: boolean;
  timestamp: number;
  raw: any;
}

export interface LarkOutgoingMessage {
  to: string;
  text: string;
  mediaUrl?: string;
  replyToId?: string;
}

export interface LarkDeliveryResult {
  channel: "lark";
  messageId: string;
  to: string;
}
