<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests;

use Symfony\Component\HttpClient\HttpClient;
use Symfony\Contracts\HttpClient\HttpClientInterface;

class Mockserver
{
    /** @var non-empty-string */
    private readonly string $serverUrl;
    private HttpClientInterface $client;

    /**
     * @param non-empty-string $serverUrl
     */
    public function __construct(string $serverUrl = 'http://mockserver:1080')
    {
        $serverUrl = rtrim($serverUrl, '/');
        assert($serverUrl !== '');
        $this->serverUrl = $serverUrl;

        $this->client = HttpClient::createForBaseUri($this->serverUrl . '/mockserver');
    }

    /**
     * @return non-empty-string
     */
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
     *         headers?: array<string, string>,
     *         body?: string,
     *     },
     *     httpResponse: array{
     *         statusCode?: int,
     *         headers?: array<string, string>,
     *         body?: string,
     *     },
     *     times?: array{
     *         remainingTimes?: int,
     *         unlimited?: bool,
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
     *     headers: array<string, string>,
     *     body?: array{rawBytes: string},
     *     keepAlive: bool,
     * }>
     */
    public function fetchRecordedRequests(array $httpRequest): array
    {
        $response = $this->client->request('PUT', 'retrieve?type=REQUESTS', [
            'body' => json_encode($httpRequest, JSON_THROW_ON_ERROR),
        ]);

        /** @var list<array{
         *     method: string,
         *     path: string,
         *     headers?: array<string, string[]>,
         *     body?: array{rawBytes: string},
         *     keepAlive: bool,
         * }> $requests
         */
        $requests = (array) json_decode($response->getContent(), true, 512, JSON_THROW_ON_ERROR);

        return array_map(
            function (array $request) {
                $headers = [];
                foreach ($request['headers'] ?? [] as $headerName => $headerValues) {
                    $headers[strtolower($headerName)] = implode(', ', $headerValues);
                }

                $request['headers'] = $headers;
                return $request;
            },
            $requests,
        );
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
