<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Apps;

use Closure;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use Keboola\ApiClientBase\Json;
use Keboola\SandboxesServiceApiClient\Exception\SandboxesServiceClientException;
use Keboola\SandboxesServiceApiClient\SandboxesErrorMessageResolver;
use Psr\Log\LoggerInterface;

class AppsApiClient
{
    private const FALLBACK_USER_AGENT = 'Keboola Sandboxes Service API PHP Client';

    private ApiClient $apiClient;

    /**
     * @param non-empty-string $baseUrl
     * @param non-empty-string $storageToken
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        string $baseUrl,
        string $storageToken,
        ?LoggerInterface $logger = null,
        int $backoffMaxTries = ApiClientOptions::DEFAULT_BACKOFF_MAX_TRIES,
        int $connectTimeout = ApiClientOptions::DEFAULT_CONNECT_TIMEOUT,
        int $requestTimeout = ApiClientOptions::DEFAULT_REQUEST_TIMEOUT,
        string $userAgent = self::FALLBACK_USER_AGENT,
        null|Closure|HandlerStack $requestHandler = null,
    ) {
        $this->apiClient = new ApiClient(
            $baseUrl,
            new StorageApiTokenAuthenticator($storageToken),
            new ApiClientOptions(
                userAgent: $userAgent,
                backoffMaxTries: $backoffMaxTries,
                connectTimeout: $connectTimeout,
                requestTimeout: $requestTimeout,
                requestHandler: $requestHandler,
                logger: $logger,
            ),
            errorMessageResolver: new SandboxesErrorMessageResolver(),
            exceptionClass: SandboxesServiceClientException::class,
        );
    }

    /**
     * @param int|null      $offset
     * @param int|null      $limit
     * @param list<string>  $types   Filter by app type (e.g. 'python', 'r', 'streamlit')
     * @return array<App>
     */
    public function listApps(
        ?int $offset = null,
        ?int $limit = null,
        array $types = [],
    ): array {
        $queryParams = [];
        if ($offset !== null) {
            $queryParams['offset'] = (string) $offset;
        }
        if ($limit !== null) {
            $queryParams['limit'] = (string) $limit;
        }
        foreach ($types as $type) {
            $queryParams['type'][] = $type;
        }

        $uri = '/apps';
        if (!empty($queryParams)) {
            $uri .= '?' . http_build_query($queryParams);
        }

        return $this->apiClient->sendRequestAndMapResponse(
            new Request('GET', $uri),
            App::class,
            isList: true,
        );
    }

    public function getApp(string $appId): App
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'GET',
                sprintf('/apps/%s', $appId),
            ),
            App::class,
        );
    }

    /**
     * @param string $appId
     * @param array<string, mixed> $payload
     */
    public function patchApp(string $appId, array $payload): void
    {
        $this->apiClient->sendRequest(
            new Request(
                'PATCH',
                sprintf('/apps/%s', $appId),
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray($payload),
            ),
        );
    }

    /**
     * @param array<string, mixed> $payload
     */
    public function createApp(array $payload): App
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                '/apps',
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray($payload),
            ),
            App::class,
        );
    }

    public function deleteApp(string $appId): void
    {
        $this->apiClient->sendRequest(
            new Request(
                'DELETE',
                sprintf('/apps/%s', $appId),
            ),
        );
    }
}
