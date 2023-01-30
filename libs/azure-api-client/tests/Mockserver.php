<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Tests;

use Symfony\Component\HttpClient\HttpClient;
use Symfony\Contracts\HttpClient\HttpClientInterface;

class Mockserver
{
    private readonly string $serverUrl;
    private HttpClientInterface $client;

    public function __construct(string $serverUrl = 'http://mockserver:1080')
    {
        $this->serverUrl = rtrim($serverUrl, '/');
        $this->client = HttpClient::createForBaseUri($this->serverUrl . '/mockserver');
    }

    public function getServerUrl(): string
    {
        return $this->serverUrl;
    }

    public function reset(): void
    {
        $this->client->request('PUT', 'reset');
    }

    /**
     * @param array{
     *     httpRequest: array{
     *         method?: string,
     *         path?: string,
     *     },
     *     httpResponse: array{
     *         statusCode?: int,
     *         headers?: array<string, string>,
     *         body?: string,
     *     },
     * } $expectation
     */
    public function expect(array $expectation): void
    {
        $this->client->request('PUT', 'expectation', [
            'body' => json_encode($expectation, JSON_THROW_ON_ERROR),
        ]);
    }

    /**
     * @param array{
     *     method?: string,
     *     path?: string,
     * } $httpRequest
     *
     * @return list<array{
     *     method: string,
     *     path: string,
     *     headers: array<string, string[]>,
     * }>
     */
    public function fetchRecordedRequests(array $httpRequest): array
    {
        $response = $this->client->request('PUT', 'retrieve?type=REQUESTS', [
            'body' => json_encode($httpRequest, JSON_THROW_ON_ERROR),
        ]);

        // @phpstan-ignore-next-line
        return (array) json_decode($response->getContent(), true, 512, JSON_THROW_ON_ERROR);
    }

    /**
     * @param array{
     *     method?: string,
     *     path?: string,
     * } $httpRequest
     */
    public function hasRecordedRequest(array $httpRequest): bool
    {
        $records = $this->fetchRecordedRequests($httpRequest);
        return count($records) > 0;
    }
}
