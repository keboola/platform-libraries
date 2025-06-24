<?php

declare(strict_types=1);

namespace Keboola\K8sClient\BaseApi;

use Keboola\K8sClient\BaseApi\Data\WatchEvent;
use Keboola\K8sClient\Util\StreamResponse;
use Kubernetes\API\Pod;
use Kubernetes\Model\Io\K8s\Apimachinery\Pkg\Apis\Meta\V1\Status;
use Psr\Http\Message\ResponseInterface;

class PodWithLogStream extends Pod
{
    public function readLog(string $namespace, string $name, array $queries = [])
    {
        /** @var ResponseInterface $response */
        $response = $this->client->request(
            'get',
            sprintf('/api/v1/namespaces/%s/pods/%s/log', $namespace, $name),
            [
                'query' => $queries,
            ],
        );

        if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300) {
            return $response->getBody();
        }

        return $this->parseResponse($response, 'readCoreV1NamespacedPodLog');
    }

    public function watch(string $namespace, string $name, array $queries = []): mixed
    {
        $readWaitTimeout = $queries['read_wait_timeout'] ?? 30;
        unset($queries['read_wait_timeout']);

        $response = $this->client->request(
            'get',
            sprintf('/api/v1/watch/namespaces/%s/pods/%s', $namespace, $name),
            [
                'stream' => true,
                'query' => $queries,
            ],
        );
        assert($response instanceof ResponseInterface);

        if ($response->getStatusCode() !== 200) {
            return $this->parseResponse($response, 'watchCoreV1NamespacedPod');
        }

        return $this->processWatch($response, $readWaitTimeout);
    }

    private function processWatch(ResponseInterface $response, int $readWaitTimeout): iterable
    {
        foreach (StreamResponse::chunkStreamResponse($response, $readWaitTimeout) as $chunk) {
            if ($chunk === null) {
                yield null;
                continue;
            }

            $eventData = json_decode($chunk, true);
            if (!is_array($eventData)) {
                continue;
            }

            yield WatchEvent::fromResponse($eventData);
        }
    }
}
