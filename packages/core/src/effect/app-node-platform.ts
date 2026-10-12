import { ModelBudget } from "../model-budget.js"
import { LLMClient, RequestExecutor } from "@opencode/ai/route"
import { Socket } from "effect/unstable/socket"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { httpClient } from "@opencode/util/effect/app-node-platform"
import { WebSocketConstructor } from "./websocket-constructor.js"
import { KV } from "../kv.js"

export const requestExecutor = makeGlobalNode({
  service: RequestExecutor.Service,
  layer: RequestExecutor.layer,
  deps: [httpClient],
})

export const llmClient = makeGlobalNode({
  service: LLMClient.Service,
  layer: ModelBudget.layer,
  deps: [requestExecutor, KV.node],
})

export const webSocketConstructor = makeGlobalNode({
  service: Socket.WebSocketConstructor,
  layer: WebSocketConstructor.layer,
  deps: [],
})

export * as LayerNodePlatform from "./app-node-platform.js"
