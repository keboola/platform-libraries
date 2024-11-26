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
    public function __construct($namespace = 'default')
    {
        $this->client = Client::getInstance();
        $this->namespace = $namespace;
    }

    public function readLog(string $namespace, string $name, array $queries = [])
    {
        $handler = new CurlHandler();
        $handlerStack = HandlerStack::create($handler);

        /** @var ResponseInterface $response */
        $response = $this->client->request(
            'get',
            sprintf('/api/v1/namespaces/%s/pods/%s/log', $namespace, $name),
            [
                'query' => $queries,
                'handler' => $handlerStack,
            ],
        );

        if ($response->getStatusCode() >= 200 && $response->getStatusCode() < 300) {
            return $response->getBody();
        }

        return $this->parseResponse($response, 'readCoreV1NamespacedPodLog');
    }
}
