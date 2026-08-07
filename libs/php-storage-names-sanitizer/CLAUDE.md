# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`README.md` shows the one-call usage. Root `CLAUDE.md` has the monorepo conventions.

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
