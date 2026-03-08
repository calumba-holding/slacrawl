<h1 align="center">slacrawl</h1>

<p align="center">
  <strong>Mirror Slack workspace data into local SQLite for fast search, structured queries, and offline inspection.</strong>
</p>

<p align="center">
  <a href="./LICENSE"><img src="https://img.shields.io/github/license/vincentkoc/slacrawl" alt="License"></a>
  <img src="https://img.shields.io/badge/go-1.25%2B-00ADD8" alt="Go 1.25+">
  <img src="https://img.shields.io/badge/storage-SQLite-003B57" alt="SQLite">
  <img src="https://img.shields.io/badge/platform-macOS%20%7C%20Linux-lightgrey" alt="Platform">
</p>

## Why slacrawl?

Slack search is convenient until you need your own workflow, your own retention, or your own queries. `slacrawl` is a local-first Go CLI that pulls Slack workspace metadata and message history into SQLite so you can inspect it without depending on the Slack UI.

Data stays on your machine. You can run it in API mode, desktop mode, or a hybrid workflow that combines both.

## What You Get

- local SQLite storage with FTS5-backed search
- workspace, channel, user, and message sync
- thread reply backfill when a user token is available
- incremental API history sync by default, with `--full` reserved for deliberate backfills
- mention extraction for structured querying
- read-only SQL access for ad hoc analysis
- `doctor` diagnostics for config, database, token, and desktop-source checks
- desktop-local ingestion of workspace metadata, channels, users, cached channel messages, drafts, read markers, recent-channel hints, and custom-status metadata
- optional Socket Mode live tailing via app token
- periodic desktop refresh with `watch`

## V1 Scope

- one workspace at a time
- public channels
- private channels
- top-level messages
- channel threads
- local FTS search
- read-only SQL access
- macOS Slack Desktop discovery

Out of scope for V1:

- Slack export ZIP import
- DMs and MPIMs
- attachment blob downloads
- write-back actions
- public Marketplace-style distribution hardening
- desktop-local message extraction beyond the documented bootstrap surface

## Requirements

- Go `1.25+`
- `node` if you want richer desktop-local IndexedDB blob decoding
- a Slack bot token for standard API sync
- an app token if you want to use `tail`
- an optional user token for fuller historical thread coverage
- macOS Slack Desktop only if you want desktop-local discovery

## Install

<details>
<summary>Build from source</summary>

```bash
git clone https://github.com/vincentkoc/slacrawl.git
cd slacrawl
go build -o bin/slacrawl ./cmd/slacrawl
./bin/slacrawl --help
```

</details>

<details>
<summary>Run without building a binary</summary>

```bash
git clone https://github.com/vincentkoc/slacrawl.git
cd slacrawl
go run ./cmd/slacrawl --help
```

</details>

## Quick Start

```bash
export SLACK_BOT_TOKEN="xoxb-..."
export SLACK_APP_TOKEN="xapp-..."
export SLACK_USER_TOKEN="xoxp-..."

go run ./cmd/slacrawl init
go run ./cmd/slacrawl doctor
go run ./cmd/slacrawl sync --source api
go run ./cmd/slacrawl search "incident"
go run ./cmd/slacrawl tail --repair-every 30m
go run ./cmd/slacrawl watch --desktop-every 5m
```

If you built the binary, replace `go run ./cmd/slacrawl` with `./bin/slacrawl`.

Choose the path that matches your setup:

- use `sync --source api` for normal incremental syncs
- use `sync --source api --full` only when you want a deliberate full backfill
- use `sync --source desktop` when you want local desktop recovery only
- use `watch` when you want desktop-local state to refresh into SQLite continuously

## Commands

- `init` creates a starter config file
- `doctor` checks config, DB access, token presence, FTS, and desktop source availability
- `sync` performs a one-shot crawl from API, desktop, or both
- `tail` listens for live events through Socket Mode
- `watch` refreshes desktop-local state on a schedule
- `search` runs local FTS queries
- `messages` lists stored messages with filters
- `mentions` lists structured mention records
- `sql` runs read-only SQL against the local database
- `users` lists synced users
- `channels` lists synced channels
- `status` prints workspace and sync status

## Default Paths

- config: `~/.slacrawl/config.toml`
- database: `~/.slacrawl/slacrawl.db`
- cache: `~/.slacrawl/cache`
- logs: `~/.slacrawl/logs`

## Configuration

The starter config lives in [`config.example.toml`](./config.example.toml). By default it points to these environment variables:

- `SLACK_BOT_TOKEN`
- `SLACK_APP_TOKEN`
- `SLACK_USER_TOKEN`

Desktop discovery is enabled by default and uses:

```text
~/Library/Containers/com.tinyspeck.slackmacgap/Data/Library/Application Support/Slack
```

Desktop config notes:

- set `[slack.desktop].enabled = false` to disable desktop ingestion
- leave `[slack.desktop].path = ""` to auto-detect the macOS Slack path
- set a custom absolute path if Slack Desktop data lives elsewhere
- set `[slack.bot]`, `[slack.app]`, or `[slack.user]` `enabled = false` to ignore that token source entirely

## Typical Workflow

```bash
go run ./cmd/slacrawl init
go run ./cmd/slacrawl sync --source api
go run ./cmd/slacrawl status
go run ./cmd/slacrawl channels
go run ./cmd/slacrawl messages --channel C12345678 --limit 20
go run ./cmd/slacrawl mentions --limit 20
go run ./cmd/slacrawl sql 'select channel_id, count(*) as messages from messages group by channel_id order by messages desc limit 10;'
```

## Notes on Coverage

- Full historical thread reply coverage in public and private channels depends on providing a user token.
- `tail` requires an app token because it uses Socket Mode.
- Desktop-local support is broader than simple discovery, but still not a full write-back or export-import path.

## Development

```bash
go test ./...
go build ./cmd/slacrawl
```

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for contribution workflow and [`SPEC.md`](./SPEC.md) for the implementation contract.

Deep-dive docs:

- [`docs/configuration.md`](./docs/configuration.md)
- [`docs/desktop-mode.md`](./docs/desktop-mode.md)

---

Built by <a href="https://github.com/vincentkoc">Vincent Koc</a> · <a href="./LICENSE">MIT</a>
