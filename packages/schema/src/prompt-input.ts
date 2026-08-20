export * as PromptInput from "./prompt-input.js"

import { Schema } from "effect"
import { AgentAttachment, PromptMention } from "./prompt.js"
import { optional, statics } from "./schema.js"
import { Skill } from "./skill.js"
import { Mcp } from "./mcp.js"

export interface FileAttachment extends Schema.Schema.Type<typeof FileAttachment> {}
export const FileAttachment = Schema.Struct({
  uri: Schema.String,
  name: Schema.String.pipe(optional),
  description: Schema.String.pipe(optional),
  mention: PromptMention.pipe(optional),
})
  .annotate({ identifier: "PromptInput.FileAttachment" })
  .pipe(
    statics((schema) => ({
      create: (input: FileAttachment) => schema.make(input),
    })),
  )

export interface Prompt extends Schema.Schema.Type<typeof Prompt> {}
export interface SkillAttachment extends Schema.Schema.Type<typeof SkillAttachment> {}
export const SkillAttachment = Schema.Struct({
  id: Skill.ID,
  mention: PromptMention.pipe(optional),
}).annotate({ identifier: "PromptInput.SkillAttachment" })

export const Prompt = Schema.Struct({
  text: Schema.String,
  files: Schema.Array(FileAttachment).pipe(optional),
  agents: Schema.Array(AgentAttachment).pipe(optional),
  skills: Schema.Array(SkillAttachment).pipe(optional),
  mcp: Mcp.RequestServers.pipe(optional).annotate({
    description:
      "Remote MCP servers available only for this prompt execution. They replace configured MCP servers and cannot use persisted OAuth.",
  }),
}).annotate({ identifier: "PromptInput" })
