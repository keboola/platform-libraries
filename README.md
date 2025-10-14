# Platform Libraries

A monorepo containing 20+ PHP libraries for the Keboola platform. All libraries are located in the `libs/` directory. Each library is independently versioned and published but developed together for easier testing and cross-library changes.

## Libraries

### Data Movement
- **input-mapping** - Downloads tables/files from Storage API (CSV/Parquet support)
- **output-mapping** - Uploads data to Storage API with import handling
- **staging-provider** - Configures staging factories for local/workspace storage
- **slicer** - Data slicing utilities

### API Clients
- **api-bundle** - Symfony bundle for Keboola API applications with authentication
- **service-client** - Generic service client for Keboola services
- **query-service-api-client** - Query Service API client
- **sync-actions-api-php-client** - Sync Actions API client
- **sandboxes-service-api-client** - Sandboxes Service API client
- **azure-api-client** - Azure API client
- **vault-api-client** - Vault API client

### Infrastructure
- **k8s-client** - Kubernetes client wrapper
- **messenger-bundle** - Symfony Messenger bundle with Azure Service Bus support
- **doctrine-retry-bundle** - Database retry logic for Doctrine
- **logging-bundle** - Logging configuration bundle

### Utilities
- **configuration-variables-resolver** - Resolves configuration variables
- **permission-checker** - Permission checking utilities
- **key-generator** - Key generation utilities
- **settle** - Settlement utilities for async operations
- **php-test-utils** - Shared testing utilities

## Development

### Environment Setup

Some libraries require environment variables for testing. Create `.env.local` from `.env`:

```bash
cp .env .env.local
# Edit .env.local with your API tokens (STORAGE_API_URL, STORAGE_API_TOKEN, etc.)
```

### Running Tests for a Library

Each library has a dedicated Docker Compose service. To work on a specific library:

```bash
# Enter library container
docker compose run --rm dev-input-mapping bash

# Inside container - install dependencies and run tests
composer install
composer tests          # PHPUnit
composer paratests      # Parallel tests (faster)
```

### Code Quality Checks

```bash
# Inside library container
composer phpcs          # Check code style
composer phpcbf         # Fix code style automatically
composer phpstan        # Static analysis
composer check          # Validation + phpcs + phpstan
composer ci             # Full CI suite (check + tests)
```

### Running Single Tests

```bash
# Run specific test file
docker compose run --rm dev-input-mapping bash -c "vendor/bin/phpunit tests/Functional/DownloadFilesTest.php"

# Run specific test method
docker compose run --rm dev-input-mapping bash -c "vendor/bin/phpunit --filter testTableManifest tests/Functional/DownloadTablesTest.php"
```

## Contributing

### Commit Message Format

This project uses [Conventional Commits v1.0.0](https://www.conventionalcommits.org/en/v1.0.0/).

**IMPORTANT:** Since this is a monorepo, commit messages MUST include the library name:

**Format:** `<type>(<library-name>): <description>`

**Examples:**
- `feat(input-mapping): add support for Parquet format downloads`
- `fix(output-mapping): resolve memory leak in large file uploads`
- `docs(service-client): update README with new endpoint examples`

## License

MIT licensed, see [LICENSE](./LICENSE) file.

## CI setup

### Auto Stop for Azure Synapse Analytics

The instructions reflect the current setup when the server is already created and belongs to the `Keboola DEV Connection Team` subscription.

Prerequisites:
* locally installed `terraform`
    * https://www.terraform.io
* configured `az` CLI tools (run `az login`)
* existing Azure Synapse Analytics

#### Prepare resources

```shell
cat <<EOF > ./provisioning/ci/synapse-auto-stop/terraform.tfvars
resource_group = "{SYNAPSE_AZURE_RESOURCE_GROUP}"
synapse_server_name = "{SYNAPSE_SERVER_NAME}"
EOF

terraform -chdir=./provisioning/ci/synapse-auto-stop init
terraform -chdir=./provisioning/ci/synapse-auto-stop apply

./provisioning/local/update-env.sh azure # or aws
```

It creates new resources in resource group of Synapse server and new application in Active Directory and will output following infromations:
- `application_account` - Application name registered in Azure Active Directory
- `runbook` - Name of the runbook with Synapse pause prodcedure
- `schedule` - Name of the schedule linked to your runbook

#### Finalize configuration

Go to the Azure Portal and then:

- In [Subscription IAM configuration](https://portal.azure.com/#@keboolaconnection.onmicrosoft.com/resource/subscriptions/eac4eb61-1abe-47e2-a0a1-f0a7e066f385/users) add your application with `Contributor` role.
  _If you do not have permissions for this operation, ask SRE team for that._
- In resource group of Synapse server find your runbook and set starts time for your schedule. 
