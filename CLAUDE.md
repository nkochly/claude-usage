# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Local dashboard for tracking Claude Code token usage, costs, and session history. Reads JSONL transcript files from `~/.claude/projects/` and stores parsed data in `~/.claude/usage.db` (SQLite).

Fork of [phuryn/claude-usage](https://github.com/phuryn/claude-usage) with fixes for token deduplication and pricing accuracy.

**Not captured:** Cowork sessions run in a local sandbox and don't write JSONL transcripts to `~/.claude/projects/`.

## Architecture

Three files, zero dependencies (stdlib only):

| File | Role |
|------|------|
| `scanner.py` | Parses JSONL transcripts into SQLite (`~/.claude/usage.db`) |
| `dashboard.py` | HTTP server + single-page HTML/JS/Chart.js dashboard |
| `cli.py` | CLI entry point: `scan`, `today`, `stats`, `dashboard` |

**Data flow:** JSONL files -> `scanner.py` (dedup by `message.id`, store in SQLite) -> `dashboard.py` (reads SQLite, serves JSON API) -> browser charts.

**Key design decisions:**
- Deduplication happens at two levels: within-file (streaming events share a `message.id`) and cross-file (UNIQUE index on `turns.message_id` catches subagent replays)
- Incremental scanning tracks file path + line count in `processed_files` table; only new lines are processed on re-scan
- Session totals are aggregated from actually-inserted turns (post-dedup) to stay consistent with the turns table

## Commands

Always use `uv run` to execute Python — never bare `python3`.

```bash
uv run python cli.py scan        # Parse JSONL files into ~/.claude/usage.db
uv run python cli.py today       # Today's usage summary (terminal)
uv run python cli.py stats       # All-time statistics (terminal)
uv run python cli.py dashboard   # Scan + launch browser dashboard on :8080
```

## Pricing

Both `cli.py` and `dashboard.py` maintain pricing tables (Anthropic API, April 2026). **These must stay in sync** — when updating one, update the other. Only models containing `opus`, `sonnet`, or `haiku` are costed; unknown models show $0 / n/a.

The code includes both current (4-6) and previous (4-5) model variants. When adding new model versions, add entries to both files:

- `cli.py`: Python dict `PRICING` (~line 18)
- `dashboard.py`: JS `const PRICING` (~line 255)

## Database Schema

Three tables in `~/.claude/usage.db`:

- **sessions**: One row per session_id. Aggregated token totals.
- **turns**: One row per API response (deduplicated by `message_id`). The `message_id` column has a conditional UNIQUE index.
- **processed_files**: Tracks which JSONL files have been scanned and how many lines were processed.

To reset: `rm ~/.claude/usage.db` and re-scan.

## Testing Changes

After modifying scanner.py, verify with:
```bash
rm ~/.claude/usage.db
uv run python cli.py scan
uv run python -c "
import sqlite3; c = sqlite3.connect('$HOME/.claude/usage.db')
t = c.execute('SELECT SUM(input_tokens), SUM(output_tokens), SUM(cache_read_tokens), SUM(cache_creation_tokens), COUNT(*) FROM turns').fetchone()
s = c.execute('SELECT SUM(total_input_tokens), SUM(total_output_tokens), SUM(total_cache_read), SUM(total_cache_creation), SUM(turn_count) FROM sessions').fetchone()
print(f'Match: {t == s}')
print(f'Dupes: {len(c.execute(\"SELECT message_id FROM turns WHERE message_id IS NOT NULL GROUP BY message_id HAVING COUNT(*)>1\").fetchall())}')
"
```

Expected: `Match: True`, `Dupes: 0`.
