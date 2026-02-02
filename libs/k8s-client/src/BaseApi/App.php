<?php

declare(strict_types=1);

namespace Keboola\K8sClient\BaseApi;

use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\App as TheApp;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V2\AppList;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use KubernetesRuntime\AbstractAPI;

/**
 * BaseApi for App custom resources (apps.keboola.com/v2)
 */
class App extends AbstractAPI
{
    /**
     * Custom response type mappings for App CRD operations
     */
    protected static function getCustomResponseTypes(): array
    {
        return [
            'listAppsKeboolaComV2NamespacedApp' => [
                '200.' => AppList::class,
            ],
            'readAppsKeboolaComV2NamespacedApp' => [
                '200.' => TheApp::class,
            ],
            'createAppsKeboolaComV2NamespacedApp' => [
                '200.' => TheApp::class,
                '201.' => TheApp::class,
                '202.' => TheApp::class,
            ],
            'patchAppsKeboolaComV2NamespacedApp' => [
                '200.' => TheApp::class,
                '201.' => TheApp::class,
            ],
            'deleteAppsKeboolaComV2NamespacedApp' => [
                '200.' => Status::class,
                '202.' => Status::class,
            ],
            'deleteAppsKeboolaComV2CollectionNamespacedApp' => [
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
                "/apis/apps.keboola.com/v2/namespaces/$namespace/apps",
                [
                    'query' => $queries,
                ],
            ),
            'listAppsKeboolaComV2NamespacedApp',
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
                "/apis/apps.keboola.com/v2/namespaces/$namespace/apps/$name",
                [
                    'query' => $queries,
                ],
            ),
            'readAppsKeboolaComV2NamespacedApp',
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
                "/apis/apps.keboola.com/v2/namespaces/$namespace/apps",
                [
                    'json' => $model->getArrayCopy(),
                    'query' => $queries,
                ],
            ),
            'createAppsKeboolaComV2NamespacedApp',
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
                "/apis/apps.keboola.com/v2/namespaces/$namespace/apps/$name",
                [
                    'json' => $model->getArrayCopy(),
                    'query' => $queries,
                ],
            ),
            'patchAppsKeboolaComV2NamespacedApp',
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
                "/apis/apps.keboola.com/v2/namespaces/$namespace/apps/$name",
                [
                    'json' => $options,
                    'query' => $queries,
                ],
            ),
            'deleteAppsKeboolaComV2NamespacedApp',
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
                "/apis/apps.keboola.com/v2/namespaces/$namespace/apps",
                [
                    'json' => $options,
                    'query' => $queries,
                ],
            ),
            'deleteAppsKeboolaComV2CollectionNamespacedApp',
        );
    }
}
