# Claude instructions for this repo

## Git workflow
- **Always push finished work to `master`** (owner's standing instruction, 2026-08-21).
  Develop wherever you like, but merge into `master` and push it before ending the task —
  don't leave work sitting only on a feature branch.

## Release conventions
- Every deployed change bumps `VERSION` (currently `2.24.x`) and adds a matching entry
  at the top of `CHANGELOG.md`. Commit messages start with `v<version>: <summary>`.
