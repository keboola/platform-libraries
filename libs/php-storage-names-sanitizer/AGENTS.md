# AGENTS.md

Guidance for AI coding agents working on the `php-storage-names-sanitizer` library.

`README.md` shows the one-call usage. Root `AGENTS.md` has the monorepo conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`php-storage-names-sanitizer` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/php-storage-names-sanitizer/`. It is published to the standalone
**[keboola/php-storage-names-sanitizer](https://github.com/keboola/php-storage-names-sanitizer)**
repository only so that Composer can install it — that repository is a **read-only mirror**. CI
re-splits the monorepo subdirectory into it on every green build and force-pushes the result, so any
commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against
  `keboola/php-storage-names-sanitizer`.** A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/php-storage-names-sanitizer/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(php-storage-names-sanitizer): …`.
- A release is a `php-storage-names-sanitizer/<version>` tag pushed in the monorepo; the mirror's tag
  is derived from it with the `php-storage-names-sanitizer/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

## Commands

Docker service `dev-php-storage-names-sanitizer` (PHP 8.2); no environment variables.

```bash
docker compose run --rm dev-php-storage-names-sanitizer composer ci      # validate + phpcs + phpstan + tests
docker compose run --rm dev-php-storage-names-sanitizer composer check   # validate + phpcs + phpstan, no tests
docker compose run --rm dev-php-storage-names-sanitizer vendor/bin/phpunit --filter testSanitize tests/ColumnNameSanitizerTest.php
```

CI also runs this library's suite with `QUERY_SERVICE__STORAGE_API_TOKEN_GCP`, but nothing in the code reads
it — the tests are pure functions over strings.

## Behaviour worth knowing

`ColumnNameSanitizer::sanitize()` is the public entry point; `Filter::filter()` is the generic mechanism
behind it (valid-character regex, replacement char, optional max length, optional lowercasing) and exists so
other name kinds can be added later without duplicating the transliteration.

The transformation order in `Filter` is significant and is what the tests pin:
transliterate to ASCII (`UnicodeString::ascii()`) → replace runs of invalid characters with the replacement
→ trim the replacement character from both ends → **additionally trim leading `_`** → truncate → lowercase.
The extra leading-underscore trim means a name that began with an invalid character never yields a
leading underscore, while a trailing one is possible.

`SYSTEM_COLUMNS` (`oid`, `tableoid`, `xmin`, `cmin`, `xmax`, `cmax`, `ctid`) are Postgres system column
names; a sanitized value that collides with one gets a `_` appended. The comparison is
case-insensitive but the suffix is applied to the already-cased value.

**This output is persisted.** Column names produced here become real Storage table columns, so any change to
the algorithm reshapes existing pipelines' output — treat it as a data-format change, not a refactor.
`output-mapping` depends on it as `*@dev` and calls it through its `Configuration\Table\Webalizer`.
