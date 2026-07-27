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

use Keboola\K8sClient\ClientFactory\StaticKubernetesApiClientFactory;
use Keboola\K8sClient\KubernetesApiClientFacade;
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
$client = KubernetesApiClientFacade::create($apiClient, $logger);

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

## Custom resources (CRDs)
The facade also serves resources it doesn't ship. Implement an API client for your CRD by extending
`Keboola\K8sClient\ApiClient\BaseNamespaceApiClient` (or `BaseClusterApiClient`), then register it via the
`$extraClients` map — keyed by the CRD model class — when building the facade:

```php
$facade = KubernetesApiClientFacade::create($apiClient, $logger, [
    My\Crd\Model::class => new My\Crd\ApiClient($apiClient),
]);

$facade->client(My\Crd\Model::class)->get('my-resource'); // typed access
$facade->mergePatch($myCrdModel);                          // generic methods route via the map too
```

## Symfony integration
The library ships no bundle, so wire the pieces as services. Minimal setup with auto-detected credentials
(env vars in dev, in-cluster ServiceAccount token in prod) using the shared-client + `create()` split:

```yaml
services:
    # credential strategy — AutoDetect needs its two sub-factories defined (RetryProxy is not autowirable)
    Keboola\K8sClient\ClientFactory\EnvVariablesKubernetesApiClientFactory:
        arguments:
            $retryProxy: !service { class: Retry\RetryProxy }
    Keboola\K8sClient\ClientFactory\InClusterKubernetesApiClientFactory:
        arguments:
            $retryProxy: !service { class: Retry\RetryProxy }
    Keboola\K8sClient\ClientFactory\AutoDetectKubernetesApiClientFactory: ~

    # the shared low-level client
    app.k8s.api_client:
        class: Keboola\K8sClient\KubernetesApiClient
        factory: ['@Keboola\K8sClient\ClientFactory\AutoDetectKubernetesApiClientFactory', 'createApiClient']
        arguments:
            $namespace: '%env(K8S_NAMESPACE)%'

    # the high-level facade (static factory called as a class-string); register CRD clients via $extraClients
    Keboola\K8sClient\KubernetesApiClientFacade:
        factory: ['Keboola\K8sClient\KubernetesApiClientFacade', 'create']
        arguments:
            $apiClient: '@app.k8s.api_client'
            $logger: '@logger'
            $extraClients: {}   # e.g. 'App\Crd\Model': '@App\Crd\ApiClient'
```

For a single credential source you can skip `AutoDetect` and point the client at
`StaticKubernetesApiClientFactory` (explicit values) or `InClusterKubernetesApiClientFactory` directly.

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
* update `KubernetesApiClientFacade::create()` to provide the new API class to the facade

## License

MIT licensed, see [LICENSE](./LICENSE) file.
