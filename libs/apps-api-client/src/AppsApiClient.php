<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient;

use Closure;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use Keboola\AppsApiClient\Exception\AppsApiClientException;
use Keboola\AppsApiClient\Model\App;
use Keboola\AppsApiClient\Model\Deployment;
use Psr\Log\LoggerInterface;

class AppsApiClient
{
    private const FALLBACK_USER_AGENT = 'Keboola Apps API PHP Client';
    private const JSON_HEADERS = ['Content-Type' => 'application/json'];

    private ApiClient $apiClient;

    /** Sends the requests that are not idempotent: a retry would repeat their side effect. */
    private ApiClient $nonRetryingApiClient;

    /**
     * @param non-empty-string $baseUrl Apps API URL, e.g. https://apps.keboola.com
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
        // ApiClient pushes its middleware onto a passed HandlerStack, so each client needs its own copy.
        $copyHandler = static fn(): null|Closure|HandlerStack => $requestHandler instanceof HandlerStack
            ? clone $requestHandler
            : $requestHandler;

        $createApiClient = static fn(ApiClientOptions $options): ApiClient => new ApiClient(
            $baseUrl,
            new StorageApiTokenAuthenticator($storageToken),
            $options,
            errorMessageResolver: new AppsApiErrorMessageResolver(),
            exceptionClass: AppsApiClientException::class,
        );

        $this->apiClient = $createApiClient(new ApiClientOptions(
            userAgent: $userAgent,
            backoffMaxTries: $backoffMaxTries,
            connectTimeout: $connectTimeout,
            requestTimeout: $requestTimeout,
            requestHandler: $copyHandler(),
            logger: $logger,
        ));
        $this->nonRetryingApiClient = $createApiClient(new ApiClientOptions(
            userAgent: $userAgent,
            backoffMaxTries: 0,
            connectTimeout: $connectTimeout,
            requestTimeout: $requestTimeout,
            requestHandler: $copyHandler(),
            logger: $logger,
        ));
    }

    public function getApp(string $appId): App
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request('GET', 'apps/' . rawurlencode($appId)),
            App::class,
        );
    }

    /**
     * Wakes the deployment that serves production. A 409 means the app has no serving deployment to wake.
     */
    public function resumeApp(string $appId): App
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request('POST', 'apps/' . rawurlencode($appId) . '/resume'),
            App::class,
        );
    }

    /**
     * Not idempotent and never retried: a repeated request creates another deployment.
     *
     * @param string|null $configVersion null deploys the latest config version
     * @param bool $promote false stages the deployment without running it
     */
    public function createDeployment(string $appId, ?string $configVersion = null, bool $promote = true): Deployment
    {
        $body = [];
        if ($configVersion !== null) {
            $body['configVersion'] = $configVersion;
        }
        if (!$promote) {
            $body['promote'] = false;
        }

        return $this->nonRetryingApiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                'apps/' . rawurlencode($appId) . '/deployments',
                self::JSON_HEADERS,
                json_encode((object) $body, JSON_THROW_ON_ERROR),
            ),
            Deployment::class,
        );
    }

    public function getDeployment(string $appId, string $deploymentId): Deployment
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request('GET', 'apps/' . rawurlencode($appId) . '/deployments/' . rawurlencode($deploymentId)),
            Deployment::class,
        );
    }
}
