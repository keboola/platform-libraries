<?php

declare(strict_types=1);

namespace Keboola\K8sClient\BaseApi;

use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\App as TheApp;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppList;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use KubernetesRuntime\AbstractAPI;

/**
 * BaseApi for App custom resources (apps.keboola.com/v1)
 */
class App extends AbstractAPI
{
    /**
     * List apps in a namespace
     */
    public function list(string $namespace, array $queries = []): AppList
    {
        return $this->parseResponse(
            $this->client->request(
                'get',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps",
                [
                    'query' => $queries,
                ],
            ),
            'listAppsKeboolaComV1NamespacedApp',
        );
    }

    /**
     * Read an app
     */
    public function read(string $namespace, string $name, array $queries = []): TheApp
    {
        return $this->parseResponse(
            $this->client->request(
                'get',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps/$name",
                [
                    'query' => $queries,
                ],
            ),
            'readAppsKeboolaComV1NamespacedApp',
        );
    }

    /**
     * Read app status
     */
    public function readStatus(string $namespace, string $name, array $queries = []): TheApp
    {
        return $this->parseResponse(
            $this->client->request(
                'get',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps/$name/status",
                [
                    'query' => $queries,
                ],
            ),
            'readAppsKeboolaComV1NamespacedAppStatus',
        );
    }

    /**
     * Create an app
     */
    public function create(string $namespace, TheApp $model, array $queries = []): TheApp
    {
        return $this->parseResponse(
            $this->client->request(
                'post',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps",
                [
                    'json' => $model,
                    'query' => $queries,
                ],
            ),
            'createAppsKeboolaComV1NamespacedApp',
        );
    }

    /**
     * Patch an app
     */
    public function patch(string $namespace, string $name, Patch $model, array $queries = []): TheApp
    {
        return $this->parseResponse(
            $this->client->request(
                'patch',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps/$name",
                [
                    'json' => $model,
                    'query' => $queries,
                ],
            ),
            'patchAppsKeboolaComV1NamespacedApp',
        );
    }

    /**
     * Delete an app
     */
    public function delete(string $namespace, string $name, DeleteOptions $options, array $queries = []): Status
    {
        return $this->parseResponse(
            $this->client->request(
                'delete',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps/$name",
                [
                    'json' => $options,
                    'query' => $queries,
                ],
            ),
            'deleteAppsKeboolaComV1NamespacedApp',
        );
    }

    /**
     * Delete a collection of apps
     */
    public function deleteCollection(string $namespace, DeleteOptions $options, array $queries = []): Status
    {
        return $this->parseResponse(
            $this->client->request(
                'delete',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps",
                [
                    'json' => $options,
                    'query' => $queries,
                ],
            ),
            'deleteAppsKeboolaComV1CollectionNamespacedApp',
        );
    }
}
