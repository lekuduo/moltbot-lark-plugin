/**
 * Lark Runtime
 * 运行时注入，用于访问 Moltbot 核心功能
 */

import type { PluginRuntime } from "moltbot/plugin-sdk";

let runtime: PluginRuntime | null = null;

export function setLarkRuntime(next: PluginRuntime) {
  runtime = next;
}

export function getLarkRuntime(): PluginRuntime {
  if (!runtime) {
    throw new Error("Lark runtime not initialized");
  }
  return runtime;
}
