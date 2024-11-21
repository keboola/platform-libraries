<?php

declare(strict_types=1);

namespace Keboola\K8sClient\ApiClient;

use Keboola\K8sClient\Exception\KubernetesResponseException;
use Keboola\K8sClient\KubernetesApiClient;
use Kubernetes\API\Pod as PodsApi;
use Kubernetes\Model\Io\K8s\Api\Core\V1\Pod;
use Kubernetes\Model\Io\K8s\Api\Core\V1\PodList;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use Psr\Http\Message\StreamInterface;

/**
 * @template-extends BaseNamespaceApiClient<PodsApi, PodList, Pod>
 */
class PodsApiClient extends BaseNamespaceApiClient
{
    public function __construct(KubernetesApiClient $apiClient, PodsApi $baseApi)
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
        $client = StreamClient::getInstance();
        $response = $client->request(
            'get',
            sprintf('/api/v1/namespaces/%s/pods/%s/log', $this->apiClient->getK8sNamespace(), $name),
            [
                'query' => $queries,
            ],
        );

        if ($response->getStatusCode() >= 400) {
            $contents = json_decode((string) $response->getBody(), true);
            if (is_array($contents) && isset($contents['message'])) {
                throw new KubernetesResponseException(
                    'K8S request has failed: ' . $contents['message'],
                    new Status($contents),
                );
            }

            throw new KubernetesResponseException(
                'K8S request has failed',
                null,
            );
        }

        return $response->getBody();
    }
}
