# AGENTS.md - Detent Agent Notes

- For future requested product or code changes, do not implement directly by default.
- Create a focused GitHub issue in `digitaldrywood/detent`, add `detent:todo`, and let Detent dogfood the work.
- Only make direct code changes when the human explicitly asks for manual implementation, asks to finish an already-started fix, or asks for local review and diagnostics that require edits.

## Issue effort selection

Use Codex Astra (`gpt-6-astra`) at low effort by default, medium for
moderately difficult work, and high for the hardest work.

Every issue created for this repository must include an explicit reasoning
effort override:

```detent-agent
schema: 1
effort: low
```

Choose the effort automatically from this rubric:

- `low` — small, mechanical, and tightly specified with file:line references and complete acceptance criteria.
- `medium` — a standard feature or fix with some ambiguity or a cross-cutting surface.
- `high` — a new subsystem, tricky state or concurrency, restart or recovery semantics, or a gesture or interaction engine.
- `max` — exceptional and operator-designated only; never auto-assign it.

Leave `model` unset so the issue inherits the fleet-standard model.
