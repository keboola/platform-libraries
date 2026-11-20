<?php

declare(strict_types=1);

namespace Keboola\AppsApiClient;

use Closure;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\StorageApiTokenAuthenticator;
use Keboola\ApiClientBase\Json;
use Keboola\AppsApiClient\Exception\AppsClientException;
use Keboola\AppsApiClient\Model\App;
use Keboola\AppsApiClient\Model\Deployment;
use Keboola\AppsApiClient\Model\Run;
use Psr\Log\LoggerInterface;

class AppsApiClient
{
    private const FALLBACK_USER_AGENT = 'Keboola Apps API PHP Client';
    private const JSON_HEADERS = ['Content-Type' => 'application/json'];

    private ApiClient $apiClient;

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
            errorMessageResolver: new AppsApiErrorMessageResolver(),
            exceptionClass: AppsClientException::class,
        );
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
     * Restarts the deployment that serves production in place: same deployment, config and code, a new
     * container and Storage token. The new container shows up as a new run. A 409 means the app has no
     * serving deployment to restart.
     */
    public function restartApp(string $appId): App
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request('POST', 'apps/' . rawurlencode($appId) . '/restart'),
            App::class,
        );
    }

    /**
     * Runs of the app, newest first.
     *
     * @param int<1, 500>|null $limit
     * @return list<Run>
     */
    public function listRuns(string $appId, ?string $deploymentId = null, ?int $limit = null): array
    {
        $query = http_build_query(array_filter(
            ['deploymentId' => $deploymentId, 'limit' => $limit],
            static fn(mixed $value): bool => $value !== null,
        ));

        return $this->apiClient->sendRequestAndMapResponse(
            new Request('GET', 'apps/' . rawurlencode($appId) . '/runs' . ($query === '' ? '' : '?' . $query)),
            Run::class,
            isList: true,
        );
    }

    /**
     * @param non-empty-string|null $configVersion null deploys the latest config version
     * @param bool $promote true promotes the deployment to production and starts it, false only stages it
     */
    public function createDeployment(string $appId, ?string $configVersion = null, bool $promote = false): Deployment
    {
        $body = [];
        if ($configVersion !== null) {
            $body['configVersion'] = $configVersion;
        }
        // Always sent: the API defaults to promoting, this method does not.
        $body['promote'] = $promote;

        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                'apps/' . rawurlencode($appId) . '/deployments',
                self::JSON_HEADERS,
                Json::encodeArray($body),
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
