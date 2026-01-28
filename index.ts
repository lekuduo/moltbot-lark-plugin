/**
 * Moltbot Lark (飞书) Channel Plugin
 * 入口文件
 */

import type { MoltbotPluginApi } from "moltbot/plugin-sdk";
import { emptyPluginConfigSchema } from "moltbot/plugin-sdk";

import { larkPlugin } from "./src/channel.js";
import { setLarkRuntime } from "./src/runtime.js";

const plugin = {
  id: "lark",
  name: "Lark (飞书)",
  description: "Lark/Feishu channel plugin with long connection support",
  configSchema: emptyPluginConfigSchema(),
  register(api: MoltbotPluginApi) {
    setLarkRuntime(api.runtime);
    api.registerChannel({ plugin: larkPlugin });
  },
};

export default plugin;
