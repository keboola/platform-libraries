# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` lists the required environment variables and the composer scripts; the root `CLAUDE.md` has the
monorepo conventions. This file covers the architecture.

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
data/metadata) — see `staging-provider/CLAUDE.md`.

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

Several tests assert on log output via the `TestHandler` set up in `setUp()`; the strategies'
`Using "…" table input staging.` messages are part of that contract.
