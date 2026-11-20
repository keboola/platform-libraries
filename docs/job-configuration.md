# Job configuration: definition, validation and schema changes

Read this before changing any configuration node in `libs/input-mapping/src/Configuration/` or
`libs/output-mapping/src/Configuration/`. Those classes are part of the job configuration contract shared by
services that are released independently of each other.

## How a job configuration is assembled

1. It starts as the component configuration stored in Storage API. A job gets it either as a reference to
   the stored configuration or inline as `configData` in the job create request.
2. It is merged with the job's runtime settings.
3. Credentials are added, usually workspace or OAuth credentials.
4. For the component, it is split into `config.json` and the table and file manifests.
5. When the job finishes, the component state is stored back as part of the configuration.

## Where the definition lives

The Symfony Config trees below are the authoritative definition of what a job configuration can contain.
Two runner paths parse it today, each with its own root tree that embeds the same mapping nodes:

- keboola/job-queue
  [`libs/job-configuration/src/JobDefinition/Configuration/ConfigurationDefinition.php`](https://github.com/keboola/job-queue/blob/main/libs/job-configuration/src/JobDefinition/Configuration/ConfigurationDefinition.php)
  (package `keboola/job-queue-job-configuration`), used by job-queue public-api, job-queue-daemon and the
  job pod (service container, input mapping and output mapping components).
- keboola/docker-bundle
  [`src/Docker/Configuration/Container.php`](https://github.com/keboola/docker-bundle/blob/main/src/Docker/Configuration/Container.php),
  used by keboola/job-runner, which is still deployed (`jobQueueRunnerImage.tag` in kbc-stacks
  `<stack>/job-queue-daemon/tag.yaml`).

Both embed the input and output mapping nodes from this repository:

- [`libs/input-mapping/src/Configuration/`](../libs/input-mapping/src/Configuration/): `Table.php`,
  `Table/Manifest.php`, `File.php`, `File/Manifest.php`;
- [`libs/output-mapping/src/Configuration/`](../libs/output-mapping/src/Configuration/): `Table.php`,
  `Table/BaseConfiguration.php`, `Table/Configuration.php`, `Table/Manifest.php`, `TableFile.php`, `File.php`,
  `File/Manifest.php`.

## Validation rules

- **Components validate forward compatibly.** A component validates only the keys it knows and accepts and
  ignores any other key, so the platform can add keys without breaking components.
- **The platform trees are strict.** The trees above reject an unknown key
  (`Unrecognized option "<key>" under "configuration.storage.output.tables.0"`). A service running an older
  library version therefore fails on a configuration produced by a newer one.
- **Normalization writes defaults.** A service that normalizes the configuration and passes it on writes
  every schema default into it, whether the user set the key or not. A new key with `defaultValue()`,
  `beforeNormalization()->ifNull()->then(...)` or `addDefaultsIfNotSet()` thus reaches every service
  downstream, including those still on the old version. Apply the default where the value is read
  (`$config['key'] ?? Default::X`) instead.

## Who parses the configuration

Besides the runners, the input and output mapping libraries are used directly by services outside the job
pipeline, which is the dependency most often missed:

- keboola/data-loader-api, which keboola/sandboxes pins, and the sandboxes component (Sandboxes / Data
  Apps);
- keboola/runner-sync-api, keboola/sandboxes-service, keboola/editor-service.

List the current consumers before every schema change, not from this page alone:

```sh
gh search code --owner keboola 'keboola/output-mapping' --filename composer.json --json repository -q '.[].repository.nameWithOwner' | sort -u
```

## Rollout order for a schema change

A change to any part of the definition above, including input and output mapping, ships in this order:

1. Add the new option without a default in the schema, and release the library (tag in this repository,
   check the version on Packagist).
2. Update every reader: docker-bundle and job-runner, the job-queue job pod (service container, input
   mapping and output mapping components, released as one unit), job-queue public-api and job-queue-daemon,
   runner-sync-api, sandboxes-service, editor-service. Within this step, a service that passes the
   normalized configuration on (for example job-queue public-api and job-queue-daemon to the job pod) goes
   after every service downstream of it.
3. Update the library in data-loader-api and release it.
4. Update the library and the data-loader-api pin in sandboxes.
5. Release the new sandboxes component version (the Developer Portal release; a tag build alone deploys
   nothing).
6. Only now add a default value for the option, if one is needed, and release the services and the UI/CLI
   that write the option.

Skipping a step makes sandboxes, or any other reader still on the old version, fail on configurations it
cannot parse, which ends in an incident. Historical context: [PST-1490](https://keboola.atlassian.net/browse/PST-1490).

The step-by-step procedure for the job-queue part (steps 2 to 5, with verification queries) is the
[mapping components release runbook](https://github.com/keboola/job-queue/blob/main/docs/runbooks/mapping-components-release.md)
in keboola/job-queue. Releasing the job pod images through kbc-stacks follows
[`apps/job-queue-daemon/docs/components-image-release.md`](https://github.com/keboola/kbc-stacks/blob/main/apps/job-queue-daemon/docs/components-image-release.md)
in keboola/kbc-stacks.
