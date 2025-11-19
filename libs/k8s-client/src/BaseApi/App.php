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
     * Custom response type mappings for App CRD operations
     */
    protected static function getCustomResponseTypes(): array
    {
        return [
            'listAppsKeboolaComV1NamespacedApp' => [
                '200.' => AppList::class,
            ],
            'readAppsKeboolaComV1NamespacedApp' => [
                '200.' => TheApp::class,
            ],
            'createAppsKeboolaComV1NamespacedApp' => [
                '200.' => TheApp::class,
                '201.' => TheApp::class,
                '202.' => TheApp::class,
            ],
            'patchAppsKeboolaComV1NamespacedApp' => [
                '200.' => TheApp::class,
                '201.' => TheApp::class,
            ],
            'deleteAppsKeboolaComV1NamespacedApp' => [
                '200.' => Status::class,
                '202.' => Status::class,
            ],
            'deleteAppsKeboolaComV1CollectionNamespacedApp' => [
                '200.' => Status::class,
            ],
        ];
    }

    /**
     * List apps in a namespace
     */
    public function list(string $namespace, array $queries = []): AppList|Status
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
    public function read(string $namespace, string $name, array $queries = []): TheApp|Status
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
     * Create an app
     */
    public function create(string $namespace, TheApp $model, array $queries = []): TheApp|Status
    {
        return $this->parseResponse(
            $this->client->request(
                'post',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps",
                [
                    'json' => $model->getArrayCopy(),
                    'query' => $queries,
                ],
            ),
            'createAppsKeboolaComV1NamespacedApp',
        );
    }

    /**
     * Patch an app
     */
    public function patch(string $namespace, string $name, Patch $model, array $queries = []): TheApp|Status
    {
        return $this->parseResponse(
            $this->client->request(
                'patch',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/apps/$name",
                [
                    'json' => $model->getArrayCopy(),
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
