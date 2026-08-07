# AGENTS.md

Guidance for AI coding agents working on the `key-generator` library.

`README.md` documents the API and the discarded-certificate caveat. Root `AGENTS.md` has the monorepo
conventions.

## Contributing — this repository is a mirror; pull requests go to the monorepo

`key-generator` is developed in the
**[keboola/platform-libraries](https://github.com/keboola/platform-libraries)**
monorepo, under `libs/key-generator/`. It is published to the standalone
**[keboola/php-key-generator](https://github.com/keboola/php-key-generator)** repository only so that
Composer can install it — that repository is a **read-only mirror**. CI re-splits the monorepo subdirectory
into it on every green build and force-pushes the result, so any commit made there is overwritten and lost.

- **Open pull requests against `keboola/platform-libraries`, never against `keboola/php-key-generator`.**
  A pull request on the mirror cannot be merged and will be closed.
- If the checkout you are in has no `libs/` directory at its root, you are in the mirror. Stop, clone
  `keboola/platform-libraries`, and make the change in `libs/key-generator/` there.
- Commit messages are Conventional Commits scoped to the library: `fix(key-generator): …`.
- A release is a `key-generator/<version>` tag pushed in the monorepo; the mirror's tag is derived from
  it with the `key-generator/` prefix stripped.
- Monorepo-wide conventions (Docker-based dev workflow, coding standards, CI layout) are in the monorepo's
  root `AGENTS.md`.

## Commands

Docker service `dev-key-generator` (PHP 8.2); requires only `ext-openssl`, no environment variables.

```bash
docker compose run --rm dev-key-generator composer ci   # validate + phpcs + phpstan + tests
docker compose run --rm dev-key-generator vendor/bin/phpunit tests/PemKeyCertificateGeneratorTest.php
```

## Notes

Keep `#[SensitiveParameter]` on any new key-bearing parameter or property — it is what keeps private keys
out of stack traces, and it is easy to drop when adding an overload.

`CertificateSigningRequest::createDefault()` hard-codes Keboola's company identity (including the company
registration number as `serialNumber`). It is only reachable through `createPemKeyCertificate()`, whose CSR
is discarded, so nothing currently depends on those values — but they are real company data, not
placeholders.

## Consumers

`staging-provider`'s `Workspace\SnowflakeKeypairGenerator` wraps this to produce Snowflake key-pair login
credentials (calling it with a `null` password), and `input-mapping` / `output-mapping` pull it in
transitively. It is depended on as `*@dev`, so a signature change breaks those at install time and CI will
run all of them on any change here.
