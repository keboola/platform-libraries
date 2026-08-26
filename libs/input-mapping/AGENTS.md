# AGENTS.md

Guidance for AI coding agents working on the `input-mapping` library.

`README.md` lists the required environment variables and the composer scripts; the root `AGENTS.md` has the
monorepo conventions. This file covers the architecture.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`input-mapping` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/input-mapping/`. It is published to the standalone
**[keboola/input-mapping](https://github.com/keboola/input-mapping)** repository only so that Composer
can install it — that repository is a **read-only mirror**. CI re-splits the monorepo subdirectory
into it on every green build and force-pushes the result, so any commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against `keboola/input-mapping`.**
  A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/input-mapping/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(input-mapping): …`.
- A release is an `input-mapping/<version>` tag pushed in the monorepo; the mirror's tag is derived from
  it with the `input-mapping/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

## Commands

Docker service `dev-input-mapping` (PHP 8.2). Beyond what the README lists:

```bash
docker compose run --rm dev-input-mapping composer check      # validate + phpcs + phpstan, no tests
docker compose run --rm dev-input-mapping vendor/bin/phpunit --testsuite CommonPart1
docker compose run --rm dev-input-mapping vendor/bin/phpunit --filter testDownloadTables tests/Functional/DownloadTablesDefaultTest.php
```

`phpunit.xml.dist` is split into backend-specific suites (`CommonPart1`, `CommonPart2`, `CommonFiles`,
`Aws`, `Azure`, `BigQuery`) that CI runs as separate jobs behind an `input-mapping-lock` concurrency group,
because they share Storage projects. Two concurrent local runs against the same project will interfere.

## Architecture

`Reader` is the entry point but holds almost no logic — it delegates to a strategy from
`Staging\StrategyFactory`.

### Strategy selection

The factory `match`es on `StagingProvider::getStagingType()` **in its constructor**, resolving two
class-strings up front: the file strategy is always `File\Strategy\Local` for every supported type, while
the table strategy is one of `Table\Strategy\{Local, S3, ABS, Snowflake, BigQuery}`. `StagingType::None`
throws. The matches are exhaustive — a new case in `keboola/staging-provider`'s `StagingType` must be
handled here.

Table strategies split into two families: `AbstractFileStrategy` (Local/S3/ABS — export to files, then
download) and `AbstractWorkspaceStrategy` (Snowflake/BigQuery — issue workspace load jobs). The workspace
family returns a `TableLoadQueueInterface` (`WorkspaceLoadQueue`, `TableExportQueue`) so loads are queued
and awaited rather than run inline.

Strategies are constructed with the four separate staging slots the provider exposes (file/table ×
data/metadata) — see `staging-provider/AGENTS.md`.

### Dev-branch rewriting

Table sources and file tags are rewritten before use so a branch job reads branch data where it exists and
production data otherwise. There are **two parallel implementations** chosen at runtime by
`Helper\TableRewriteHelperFactory` / `Helper\TagsRewriteHelperFactory` based on
`ClientOptions::useBranchStorage()`:

- `RealDevStorage*` — branch buckets have their own identity
- `FakeDevStorage*` — legacy, branch buckets are name-prefixed clones

Both must stay behaviourally aligned; each has a mirrored test under `tests/Helper/`.

`Reader` additionally runs `Helper\InputBucketValidator::checkDevBuckets()`, but only when dev inputs are
disabled *and* branch storage is off — on protected-branch projects dev and prod bucket names coincide, so
the check would be meaningless.

### Options, state and results

Raw configuration arrays are normalized through the `Configuration\*` Symfony Config trees into
`*Options` / `*OptionsList` value objects. The `Rewritten*` variants are produced *after* branch rewriting,
so a signature taking `RewrittenInputTableOptionsList` is asserting that rewriting already happened — that
type distinction is the only guard against using un-rewritten sources.

`State\Input{File,Table}StateList` carries incremental-processing state across runs; `downloadFiles()`
returns the updated list for the caller to persist. `Table\Result` + `Metrics`/`TableMetrics` are what the
runner reports as job metrics.

## Tests

`tests/AbstractTestCase.php` provisions Storage fixtures declaratively: annotate a test method with an
attribute from `tests/Needs/` (`NeedsEmptyInputBucket`, `NeedsDevBranch`, `NeedsTestTables`, …) and
`TestSatisfyer` reflects over it, creates the resources and assigns ids to the matching `$this->…Id`
properties. Add a fixture kind by adding an attribute plus a branch in `TestSatisfyer` — don't create
buckets ad hoc inside test methods.

Several tests assert on log output via the `TestHandler` set up in `setUp()`. The strategies'
`Using "…" table input staging.` messages are part of that contract, and so is the whole job-log
message sequence documented in `README.md`.

**The level split is the contract.** `info` carries the stable, greppable messages — `Fetched table
<source>.`, `All tables were fetched.`, `Processing N … table exports.`, `Processed N workspace
exports.`, `Fetched file "<name>".`, `All files were fetched.` — with their original text. Support
greps job logs for them, so they may never be rewritten and nothing may be appended to them; a new
message is added as a new line, not by extending an existing one. All timing, size, throughput and
Storage job/file detail goes to `debug`, as a standalone line that repeats the identifier it describes
so it reads on its own. Assert on the new detail with `hasDebugThatContains()`, never
`hasInfoThatContains()`.

`Table\Strategy\AbstractStrategy::logTableFetched()` owns both halves of the per-table line (the info
message and its optional debug detail); go through it rather than building either message inline.

Per-table timing works because `TableExporter::downloadExportedFiles()` is a plain loop over the job
results, so `Table\Strategy\Local` calls it once per export job. If that ever changes upstream, the
per-table durations silently become one batch again — `tests/Table/Strategy/LocalStrategyLoggingTest.php`
is the guard; it also asserts that the info level contains *exactly* the three unchanged contract
messages, so anything new leaking into `info` fails there.
