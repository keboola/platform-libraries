# AGENTS.md

Guidance for AI coding agents working on the `output-mapping` library.

`README.md` covers what the library does functionally (tables vs files, slicing rules, description
handling) and the required environment variables; the root `AGENTS.md` has the monorepo conventions. This
file covers the code structure.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`output-mapping` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/output-mapping/`. It is published to the standalone
**[keboola/output-mapping](https://github.com/keboola/output-mapping)** repository only so that Composer
can install it — that repository is a **read-only mirror**. CI re-splits the monorepo subdirectory
into it on every green build and force-pushes the result, so any commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against `keboola/output-mapping`.**
  A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/output-mapping/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(output-mapping): …`.
- A release is a `output-mapping/<version>` tag pushed in the monorepo; the mirror's tag is derived from
  it with the `output-mapping/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

## Commands

Docker service `dev-output-mapping` (PHP 8.2). Beyond the README's list:

```bash
docker compose run --rm dev-output-mapping composer check     # validate + phpcs + phpstan, no tests
docker compose run --rm dev-output-mapping vendor/bin/phpunit --testsuite workspace-writer-tests
docker compose run --rm dev-output-mapping vendor/bin/phpunit --filter testWriteTable tests/Writer/StorageApiLocalTableWriterTest.php
```

`phpunit.xml.dist` is carved into suites (`general-tests`, `main-writer-tests-1/2`,
`workspace-writer-tests`, `native-types`, `new-native-types`, `slice`) that CI runs as separate jobs behind
an `output-mapping-lock` concurrency group, since they share Storage projects. The suites are defined by
exclusion, so **a new file under `tests/Writer/` lands in `main-writer-tests-2` by default** — check that
this is where you want it, and that you haven't accidentally excluded it from every suite.

`composer install` runs `Keboola\Slicer\Slicer::installSlicer` on `pre-autoload-dump`, downloading the
`bin/slicer` binary. Slicing tests fail with a missing-binary error if dependencies were installed with
`--no-scripts`.

## Architecture

`TableLoader::uploadTables()` is the table pipeline; `Writer\FileWriter` is the (much simpler) file
pipeline. Both get their strategy from `Staging\StrategyFactory`.

### Strategy selection

The factory `match`es on `StagingProvider::getStagingType()` in its constructor. Unlike input-mapping, only
three types are supported — `Local`, `WorkspaceSnowflake`, `WorkspaceBigquery`; `S3`/`Abs` throw
`InvalidOutputException` because output never writes to object storage directly. Files always use
`Writer\File\Strategy\Local`; tables use `LocalTableStrategy` (upload gzipped CSV) or
`SqlWorkspaceTableStrategy` (load from workspace objects, no upload and no slicing).

### The mapping value-object chain

Configuration flows through a deliberate sequence of types, and the type name tells you how far along the
pipeline you are:

`MappingFromRawConfiguration` → `MappingFromRawConfigurationAndPhysicalData` (matched with files on disk /
workspace objects) → `…WithManifest` (manifest located) → `MappingFromProcessedConfiguration` (manifest and
mapping merged, webalized, branch-rewritten, validated).

`MappingCombiner\{Local,Workspace}MappingCombiner` produces the physical-data pairing and
`SourcesValidator\{Local,Workspace}SourcesValidator` checks it, both selected by staging. When adding a
step, extend the chain rather than mutating an earlier type — downstream code relies on the guarantees a
given class name implies.

### Feature-flag gating

`OutputMappingSettings` is the single place that reads project features off the `StorageApiToken`
(`output-mapping-slice`, `tag-staging-files`, `native-types`, `new-native-types`,
`bigquery-native-types`, `output-mapping-connection-webalize`) and the `dataTypeSupport` mode
(`authoritative` / `hints` / `none`). Query behaviour through its `has*Feature()` methods rather than
inspecting the token elsewhere.

### Deferred loads

Loads are not executed inline. `LoadTableTaskCreator` builds `LoadTableTask` /
`CreateAndLoadTableTask` objects, they are collected into a `DeferredTasks\LoadTableQueue`, and metadata
(`ColumnsMetadata`, `SchemaColumnsMetadata`, `TableMetadata`) is applied after the load jobs finish. This
is what lets many tables load in parallel and what makes `FailedLoadTableDecider` possible.

### Failed-job semantics

`TableLoader` runs in a degraded mode when `OutputMappingSettings::isFailedJob()` is true: configuration
resolution errors are swallowed instead of thrown, and only sources with `write_always = true` are
uploaded. Any new validation added to the loop must respect that branch, or failed jobs will start
throwing where they previously wrote their partial output.

### Table structure reconciliation

`Storage\TableStructureValidatorFactory` picks a validator per destination table
(`TableStructureValidator` vs `TypedTableStructureValidator`), and `Storage\TableStructureModifier*` /
`TableDataModifier` / `TableDescriptionModifier` apply the resulting diff to an existing table.
`Storage\NativeTypeDecisionHelper` decides whether a table is created as typed or untyped — that decision
is made once and everything downstream follows it.
