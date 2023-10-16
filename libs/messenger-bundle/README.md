# Keboola Messenger Bundle
Symfony Messenger bundle preconfigured for Keboola platform.

## Installation
Install the package with Composer:
```shell
composer require keboola/messenger-bundle
```

## Configuration
Bundle supportf following configuration options:
* **platform** - required, `aws`, `azure` or `gcp`
* **connection_events_queue_dsn** - required, DSN for connection events queue
  * for AWS use the queue URL, ex. `https://sqs.eu-central-1.amazonaws.com/025303414634/queue-name`
  * for Azure use format `azure://<sas-key-name>:<sas-kev-value>@<servicebus-namespace>?entity_name=<servicebus-queue-name>`,
    see https://github.com/AymDev/MessengerAzureBundle for details

Reference:
```yaml
keboola_messenger:
  platform: "%env(PLATFORM)%"
  connection_events_queue_dsn: "%env(CONNECTION_EVENTS_QUEUE_DSN)%"
```

## Development
Prerequisites:
* `aws` CLI with configured `Keboola-Dev-Platform-Services-AWSAdministratorAccess` profile
* `az` CLI with configured for Keboola DEV Platform Services Team subscription
    * run `az account set --subscription c5182964-8dca-42c8-a77a-fa2a3c6946ea`
* installed `terraform` (https://www.terraform.io) and `jq` (https://stedolan.github.io/jq) to setup local env
* installed `docker` and `docker-compose` to run & develop the app

TL;DR:
```bash
export NAME_PREFIX= # your name/nickname to make your resource unique & recognizable

cat <<EOF > ./provisioning/local/terraform.tfvars
name_prefix = "${NAME_PREFIX}"
EOF

terraform -chdir=./provisioning/local init -backend-config="key=messenger-bundle/${NAME_PREFIX}.tfstate"
terraform -chdir=./provisioning/local apply
./provisioning/local/update-env.sh aws # or azure

docker-compose run --rm dev-messenger-bundle composer install
docker-compose run --rm dev-messenger-bundle composer ci
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
