<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient\Sandboxes;

use Closure;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use Keboola\ApiClientBase\Json;
use Keboola\SandboxesServiceApiClient\Exception\SandboxesServiceClientException;
use Keboola\SandboxesServiceApiClient\Sandboxes\Legacy\Project;
use Keboola\SandboxesServiceApiClient\Sandboxes\Legacy\Sandbox;
use Keboola\SandboxesServiceApiClient\SandboxesErrorMessageResolver;
use Psr\Log\LoggerInterface;

class SandboxesApiClient
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

    public function getSandbox(string $sandboxId): Sandbox
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'GET',
                sprintf('/sandboxes/%s', $sandboxId),
            ),
            Sandbox::class,
        );
    }

    public function createSandbox(array $payload): Sandbox
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                '/sandboxes',
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray($payload),
            ),
            Sandbox::class,
        );
    }

    public function deleteSandbox(string $sandboxId): void
    {
        $this->apiClient->sendRequest(
            new Request(
                'DELETE',
                sprintf('/sandboxes/%s', $sandboxId),
            ),
        );
    }

    public function updateSandbox(string $sandboxId, array $array): Sandbox
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'PATCH',
                sprintf('/sandboxes/%s', $sandboxId),
                [
                    'Content-Type' => 'application/json',
                ],
                Json::encodeArray($array),
            ),
            Sandbox::class,
        );
    }

    public function getCurrentProject(): Project
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'GET',
                '/sandboxes/project',
            ),
            Project::class,
        );
    }
}
