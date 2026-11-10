# AGENTS.md

<!-- house-rules:start source=bsm/misc/agents/house-rules.md -->
## House rules

These apply in every bsm repository. A repository may add stricter rules,
not weaker ones. The source is `agents/house-rules.md` in `bsm/misc`:
change it there, then copy it into each repository's `AGENTS.md`.

1. **A person is accountable** for what their agent opens or comments.
2. **Verify, don't guess.** Open a definition before using it. Confirm a
   cause in the code, even when someone names the location.
3. **Keep changes narrow.** Change what was asked, where the problem is. A
   fix comes with a test that fails without it.
4. **Keep it lean.** DRY within reason, no speculative helpers. Test
   non-obvious behaviour, not trivial paths.
5. **Comments say why.** Document behaviour next to the code, not in a
   README. A guard says what it prevents; read that before removing it.
6. **Review your diff before a pull request.** Identifiers exist as used,
   nothing unrelated changed, the description is true, breaking API
   changes are flagged.
7. **Report only what ran**, including skipped or failed checks.
8. **CI is shared.** Workflows call `bsm/misc`. Don't change CI in a
   feature or fix pull request.
9. **Keep private things out.** No secrets, private names, URLs or data.
10. **No agent attribution** in commits, pull requests, comments or docs.
11. **Write plainly.** Simple English, short sentences, plain ASCII
    punctuation, no Oxford comma. Summaries are a few short bullets.

### Go repositories

- Follow Effective Go and Go Code Review Comments.
- Tests for `foo.go` go in `foo_test.go`, runnable examples in
  `example_test.go`.
<!-- house-rules:end -->

## This repository

See `CLAUDE.md`.
