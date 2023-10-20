# Keboola Messenger Bundle
Symfony Messenger bundle preconfigured for Keboola platform.

## Installation
Install the package with Composer:
```shell
composer require keboola/messenger-bundle
```

## Configuration
Bundle supports following configuration options:
* **platform** - optional, `aws`, `azure`, `gcp` or `null`
  * when not set (or set to `null`) the bundle will not set up any transport 
* **connection_events_queue_dsn** - optional, DSN for connection events queue
* **connection_audit_log_queue_dsn** - optional, DSN for connection audit log queue
 
See documentation of each platform driver for details on DSN syntax
  * for AWS use the queue URL, ex. `https://sqs.eu-central-1.amazonaws.com/025303414634/queue-name`,
    see https://symfony.com/doc/current/messenger.html#amazon-sqs for details
  * for Azure use format `azure://<sas-key-name>:<sas-kev-value>@<servicebus-namespace>?entity_path=<servicebus-queue-name>`,
    see https://github.com/AymDev/MessengerAzureBundle for details
  * for GCP use format `gps://default/<topic-name>`,
    see https://github.com/petitpress/gps-messenger-bundle for details

Reference:
```yaml
keboola_messenger:
  platform: "%env(PLATFORM)%"
  connection_events_queue_dsn: "%env(CONNECTION_EVENTS_QUEUE_DSN)%"
  connection_audit_log_queue_dsn: "%env(CONNECTION_AUDIT_LOG_QUEUE_DSN)%"
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

## Reverse-engineering events
If a problem is found in existing serializer or a new serializer needs to be implemented to support new platform,
it is possible to connect to a real queue on testing environment to introspect the events structure.

The procedure is slightly different for each platform, but the general idea is the same:
1. Open the testing account/project/env in web console (https://portal.azure.com, https://console.cloud.google.com etc.)
2. Configure queue for the kind of event you want to debug
   * on GCP create a new subscription to existing topic
     * in Pub/Sub open topic you want to subscribe to
     * in detail, go to Subscriptions tab and click "Create subscription"
   * on AWS create a new SQS queue and subscribe it to existing SNS topic
     * go to SQS and create a new queue
     * go to queues list, select the queue and in "Actions" menu select "Subscribe to SNS topic"
   * on Azure create a new Service Bus and subscribe it to existing Event Grid topic
3. Manually update `.env.local` configuration to connect to the queue
   * set queue DSN to the queue created in previous step
   * on GCP you can use your local application default credentials
     1. copy contents of `~/.config/gcloud/application_default_credentials.json` to `./var/gcp/private-key.json`
     2. set extra ENV `GCLOUD_PROJECT`
4. Run `php tests/console messenger:consume <transport_name> -vvv --limit 1` to receive one event from the queue

If the event is successfully consumed, you can see its contents on the output.

When you are done, don't forget to clean up after yourself and delete the queue.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
