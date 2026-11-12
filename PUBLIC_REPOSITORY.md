# Public repository release checklist

Commit source, dependency definitions, synthetic fixtures, and documentation that describes reproducible procedures. Use example paths relative to the project or placeholders. Review what a diff reveals about the operator, the workstation, and individual studies.

Keep credentials, private support correspondence, approved receipt manifests, provider responses, source-derived operational values, private registration and ledger identifiers, checkpoints, account results, and notification state in their existing local stores. The root Kraken commissioning manifest is intentionally ignored in place because the legacy recovery command reads it there. Live grant evidence stays outside the repository as required by the source loader. The detailed synthetic-paper commissioning report is local. [Public synthetic protocol guide](EMA_SYNTHETIC_PAPER_GUIDE.md)

Before a manual commit, explicitly stage the reviewed filenames and inspect the staged path list and diff.

```powershell
git status --short --ignored
git diff --cached --name-only
git diff --cached --check
git diff --cached
```

Ignore rules apply to untracked files. If a private file is already tracked, replace it with a reviewed public summary or remove it from the proposed public tree after preserving the local original. Avoid adding ignored files with `git add -f`. Current-tree redactions leave previously published blobs reachable in Git history. Investigate prior disclosures independently, and rotate any exposed credentials if one is found.
