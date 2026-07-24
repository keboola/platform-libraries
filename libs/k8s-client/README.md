# Kubernetes Client

High-level K8S client library. It is based on `kubernetes/php-client` library, but enhances
it in many ways:
* support connection to multiple clusters
* automatic handling of result type (you don't need to check if the result is what you expect, `Status` or something else)
* integrated retries in case of networking problems
* high-level operations like `create` multiple resources at once, `waitWhileExists` to wait while given resource exists etc.

## Usage
To create a client, first pick a `Keboola\K8sClient\ClientFactory\KubernetesApiClientFactory` implementation that
matches how you obtain credentials, then use it together with the universal `KubernetesApiClientFacadeFactory` to
build the high-level facade:
* `StaticKubernetesApiClientFactory` if you have explicit cluster credentials
* `InClusterKubernetesApiClientFactory` if you run inside a Pod which has access to K8S API
* `EnvVariablesKubernetesApiClientFactory` if credentials are provided via `K8S_HOST`/`K8S_TOKEN`/`K8S_CA_CERT_PATH`/`K8S_NAMESPACE` env variables
* `AutoDetectKubernetesApiClientFactory` to try env variables first, falling back to in-cluster credentials

```php
<?php

use Keboola\K8sClient\ClientFactory\KubernetesApiClientFacadeFactory;
use Keboola\K8sClient\ClientFactory\StaticKubernetesApiClientFactory;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Container;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;

$clientFactory = new StaticKubernetesApiClientFactory(
    $retryProxy,
    'https://api.k8s-cluster.example.com',
    'secret-token',
    'var/k8s/caCert.pem',
    'default',
);
$apiClient = $clientFactory->createApiClient();
$client = (new KubernetesApiClientFacadeFactory($logger))->create($apiClient);

$pod = new Pod([
    'metadata' => [
        'name' => 'my-pod',
    ],
    'spec' => [
        'restartPolicy' => 'Never',
        'containers' => [
            new Container([
                'name' => 'app',
                'image' => 'alpine',
                'command' => ['sh', '-c', 'echo hello; sleep 3; echo bye'],
            ]),
        ],
    ],
]);

// create the pod
$client->createModels([
    $pod,
]);

// wait for pod to finish
do {
    $pod = $client->pods()->getStatus($pod->metadata->name);

    if (in_array($pod->status->phase, ['Succeeded', 'Failed'], true)) {
        break;
    }

    sleep(1);
} while (true);

// check pod logs
$client->pods()->readLog($pod->metadata->name);

// delete the pod
$client->deleteModels([
    $pod,
]);
```

## Development
Prerequisites:
* configured `az` and `aws` CLI tools (run `az login` and `aws configure --profile keboola-dev-platform-services`)
* installed `terraform` (https://www.terraform.io) and `jq` (https://stedolan.github.io/jq) to setup local env
* installed `docker` to run & develop the library

TL;DR:
```bash
export NAME_PREFIX= # your name/nickname to make your resource unique & recognizable

cat <<EOF > ./provisioning/local/terraform.tfvars
name_prefix = "${NAME_PREFIX}"
EOF

terraform -chdir=./provisioning/local init -backend-config="key=k8s-client/${NAME_PREFIX}.tfstate"
terraform -chdir=./provisioning/local apply
./provisioning/local/update-env.sh azure # or aws

docker compose run --rm dev composer install
docker compose run --rm dev composer ci
```

## Implementing new API
Only few K8S APIs we needed are implement so far. To implement new API, do following:
* create API client wrapper in `Keboola\K8sClient\ApiClient`
  * this is a wrapper around `kubernetes/php-client` API class, takes care of handling results
* add the wrapper to `KubernetesApiClientFacade`
  * inject the `kubernetes/php-client` client through constructor
  * add support for the new resource to methods signatures
* update `KubernetesApiClientFacadeFactory::create()` to provide new API class to `KubernetesApiClientFacade`

## License

MIT licensed, see [LICENSE](./LICENSE) file.
