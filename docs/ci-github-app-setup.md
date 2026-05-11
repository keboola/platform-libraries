# GitHub App setup for library publishing

The `_split-library.yml` workflow pushes to standalone `keboola/<library>` repos
using a short-lived installation token from a GitHub App. This document covers
the one-time setup steps. Run these before merging the migration PR.

## Create the App

1. Go to https://github.com/organizations/keboola/settings/apps and click **New GitHub App**.
2. Fill in:
   - **Name:** `keboola-libs-publisher`
   - **Homepage URL:** the URL of this repo
   - **Webhook → Active:** unchecked
   - **Repository permissions:**
     - **Contents:** Read and write
     - **Metadata:** Read-only (auto-selected)
   - **Where can this GitHub App be installed?** Only on this account
3. Create the App and note the **App ID** shown on the next page.
4. Scroll to **Private keys** and click **Generate a private key**. A `.pem` file downloads.

## Install on standalone repos

1. From the App page, click **Install App** → choose the `keboola` org.
2. Pick **Only select repositories** and tick all 22 `keboola/<lib>` standalone repos:
   - api-bundle, azure-api-client, configuration-variables-resolver,
     doctrine-retry-bundle, git-service-php-api-client, input-mapping,
     k8s-client, logging-bundle, messenger-bundle, output-mapping,
     permission-checker, php-key-generator, php-storage-names-sanitizer,
     php-test-utils, query-service-api-php-client,
     sandboxes-service-api-php-client, service-client, settle, slicer,
     staging-provider, sync-actions-api-php-client, vault-api-php-client
3. Confirm.

## Add credentials to `platform-libraries`

1. Visit https://github.com/keboola/platform-libraries/settings/variables/actions
2. Click **New repository variable**, name it `LIBS_PUBLISHER_APP_ID`, paste the App ID.
3. Visit https://github.com/keboola/platform-libraries/settings/secrets/actions
4. Click **New repository secret**, name it `LIBS_PUBLISHER_APP_PRIVATE_KEY`, paste the entire contents of the `.pem` file (including `-----BEGIN…` and `-----END…` lines).

## Verification

After cutover, the `_split-library.yml` workflow should succeed on the first push that affects any library. If it fails with `403` or `Resource not accessible by integration`, double-check:
- The App is installed on the target repo
- The App has `Contents: Read and write` permission
- `LIBS_PUBLISHER_APP_PRIVATE_KEY` includes the full PEM block
