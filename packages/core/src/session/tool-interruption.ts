// A retained pending call has an uncertain outcome after restart. This fixed
// status never asserts that an external side effect did or did not occur.
export const interruptedToolError = Object.freeze({
  type: "aborted" as const,
  message: "The result is unavailable. This action may already have run; inspect its outcome before retrying.",
})
