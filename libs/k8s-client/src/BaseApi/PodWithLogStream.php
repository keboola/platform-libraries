<?php

declare(strict_types=1);

namespace Keboola\K8sClient\BaseApi;

use GuzzleHttp\Handler\CurlHandler;
use GuzzleHttp\HandlerStack;
use Kubernetes\API\Pod;
use KubernetesRuntime\Client;
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
                // use custom handler stack to prevent loading whole response by logger
                'handler' => HandlerStack::create(),
            ],
        );

        if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300) {
            return $response->getBody();
        }

        return $this->parseResponse($response, 'readCoreV1NamespacedPodLog');
    }
}
