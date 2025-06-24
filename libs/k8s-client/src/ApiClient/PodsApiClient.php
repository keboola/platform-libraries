<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Generator;
use Keboola\K8sClient\BaseApi\PodWithLogStream;
use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PodList;
use Psr\Http\Message\StreamInterface;

/**
 * @template-extends BaseNamespaceApiClient<PodWithLogStream, PodList, Pod>
 */
class PodsApiClient extends BaseNamespaceApiClient
{
    public function __construct(KubernetesApiClient $apiClient, PodWithLogStream $baseApi)
    {
        parent::__construct(
            $apiClient,
            $baseApi,
            PodList::class,
            Pod::class,
        );
    }

    public function getStatus(string $name, array $queries = []): Pod
    {
        return $this->apiClient->request($this->baseApi, 'readStatus', Pod::class, $name, $queries);
    }

    public function readLog(string $name, array $queries = []): StreamInterface
    {
        $response = $this->apiClient->request(
            $this->baseApi,
            'readLog',
            StreamInterface::class,
            $name,
            $queries,
        );

        return $response;
    }

    public function watch(string $name, array $queries = []): iterable
    {
        yield from $this->apiClient->request(
            $this->baseApi,
            'watch',
            Generator::class,
            $name,
            $queries,
        );
    }
}
