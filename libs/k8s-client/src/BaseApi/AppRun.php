<?php

declare(strict_types=1);

namespace Keboola\K8sClient\BaseApi;

use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRun as TheAppRun;
use Keboola\K8sClient\Model\Io\Keboola\Apps\V1\AppRunList;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\DeleteOptions;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Patch;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use KubernetesRuntime\AbstractAPI;

/**
 * BaseApi for AppRun custom resources (apps.keboola.com/v1)
 */
class AppRun extends AbstractAPI
{
    /**
     * Custom response type mappings for AppRun CRD operations
     */
    protected static function getCustomResponseTypes(): array
    {
        return [
            'listAppsKeboolaComV1NamespacedAppRun' => [
                '200.' => AppRunList::class,
            ],
            'readAppsKeboolaComV1NamespacedAppRun' => [
                '200.' => TheAppRun::class,
            ],
            'createAppsKeboolaComV1NamespacedAppRun' => [
                '200.' => TheAppRun::class,
                '201.' => TheAppRun::class,
                '202.' => TheAppRun::class,
            ],
            'patchAppsKeboolaComV1NamespacedAppRun' => [
                '200.' => TheAppRun::class,
                '201.' => TheAppRun::class,
            ],
            'deleteAppsKeboolaComV1NamespacedAppRun' => [
                '200.' => Status::class,
                '202.' => Status::class,
            ],
            'deleteAppsKeboolaComV1CollectionNamespacedAppRun' => [
                '200.' => Status::class,
            ],
        ];
    }

    /**
     * List appruns in a namespace
     */
    public function list(string $namespace, array $queries = []): AppRunList|Status
    {
        return $this->parseResponse(
            $this->client->request(
                'get',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/appruns",
                [
                    'query' => $queries,
                ],
            ),
            'listAppsKeboolaComV1NamespacedAppRun',
        );
    }

    /**
     * Read an apprun
     */
    public function read(string $namespace, string $name, array $queries = []): TheAppRun|Status
    {
        return $this->parseResponse(
            $this->client->request(
                'get',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/appruns/$name",
                [
                    'query' => $queries,
                ],
            ),
            'readAppsKeboolaComV1NamespacedAppRun',
        );
    }

    /**
     * Create an apprun
     */
    public function create(string $namespace, TheAppRun $model, array $queries = []): TheAppRun|Status
    {
        return $this->parseResponse(
            $this->client->request(
                'post',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/appruns",
                [
                    'json' => $model->getArrayCopy(),
                    'query' => $queries,
                ],
            ),
            'createAppsKeboolaComV1NamespacedAppRun',
        );
    }

    /**
     * Patch an apprun
     */
    public function patch(string $namespace, string $name, Patch $model, array $queries = []): TheAppRun|Status
    {
        return $this->parseResponse(
            $this->client->request(
                'patch',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/appruns/$name",
                [
                    'json' => $model->getArrayCopy(),
                    'query' => $queries,
                ],
            ),
            'patchAppsKeboolaComV1NamespacedAppRun',
        );
    }

    /**
     * Delete an apprun
     */
    public function delete(string $namespace, string $name, DeleteOptions $options, array $queries = []): Status
    {
        return $this->parseResponse(
            $this->client->request(
                'delete',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/appruns/$name",
                [
                    'json' => $options,
                    'query' => $queries,
                ],
            ),
            'deleteAppsKeboolaComV1NamespacedAppRun',
        );
    }

    /**
     * Delete a collection of appruns
     */
    public function deleteCollection(string $namespace, DeleteOptions $options, array $queries = []): Status
    {
        return $this->parseResponse(
            $this->client->request(
                'delete',
                "/apis/apps.keboola.com/v1/namespaces/$namespace/appruns",
                [
                    'json' => $options,
                    'query' => $queries,
                ],
            ),
            'deleteAppsKeboolaComV1CollectionNamespacedAppRun',
        );
    }
}
