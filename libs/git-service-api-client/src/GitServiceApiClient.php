<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

use Closure;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Request;
use Keboola\ApiClientBase\ApiClient;
use Keboola\ApiClientBase\ApiClientOptions;
use Keboola\ApiClientBase\Auth\KeboolaServiceAccountAuthenticator;
use Keboola\ApiClientBase\Auth\ManageApiTokenAuthenticator;
use Keboola\ApiClientBase\Json;
use Keboola\GitServiceApiClient\Exception\GitServiceClientException;
use Keboola\GitServiceApiClient\Model\CreatedCredential;
use Keboola\GitServiceApiClient\Model\Credential;
use Keboola\GitServiceApiClient\Model\CredentialListWrapper;
use Keboola\GitServiceApiClient\Model\Repository;
use Psr\Log\LoggerInterface;

class GitServiceApiClient
{
    private const FALLBACK_USER_AGENT = 'Keboola Git Service PHP Client';
    private const JSON_HEADERS = ['Content-Type' => 'application/json'];

    private ApiClient $apiClient;

    /**
     * @param non-empty-string $baseUrl
     * @param non-empty-string|null $manageToken
     * @param int<0, max> $backoffMaxTries
     *
     * When $manageToken is provided, authenticates with X-KBC-ManageApiToken.
     * When null (default), authenticates via the projected Kubernetes ServiceAccount
     * token at the default path — see {@see KeboolaServiceAccountAuthenticator}.
     */
    public function __construct(
        string $baseUrl,
        ?string $manageToken = null,
        ?LoggerInterface $logger = null,
        int $backoffMaxTries = ApiClientOptions::DEFAULT_BACKOFF_MAX_TRIES,
        int $connectTimeout = ApiClientOptions::DEFAULT_CONNECT_TIMEOUT,
        int $requestTimeout = ApiClientOptions::DEFAULT_REQUEST_TIMEOUT,
        string $userAgent = self::FALLBACK_USER_AGENT,
        null|Closure|HandlerStack $requestHandler = null,
    ) {
        $authenticator = $manageToken !== null
            ? new ManageApiTokenAuthenticator($manageToken)
            : new KeboolaServiceAccountAuthenticator();

        $this->apiClient = new ApiClient(
            $baseUrl,
            $authenticator,
            new ApiClientOptions(
                userAgent: $userAgent,
                backoffMaxTries: $backoffMaxTries,
                connectTimeout: $connectTimeout,
                requestTimeout: $requestTimeout,
                requestHandler: $requestHandler,
                logger: $logger,
            ),
            errorMessageResolver: new GitServiceErrorMessageResolver(),
            exceptionClass: GitServiceClientException::class,
        );
    }

    public function createRepository(string $name): Repository
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                'repos',
                self::JSON_HEADERS,
                Json::encodeArray(['name' => $name]),
            ),
            Repository::class,
        );
    }

    public function getRepository(string $name): Repository
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request('GET', 'repos/' . rawurlencode($name)),
            Repository::class,
        );
    }

    public function deleteRepository(string $name): void
    {
        $this->apiClient->sendRequest(
            new Request('DELETE', 'repos/' . rawurlencode($name)),
        );
    }

    /**
     * @return list<Credential>
     */
    public function listCredentials(string $repo): array
    {
        $wrapper = $this->apiClient->sendRequestAndMapResponse(
            new Request('GET', 'repos/' . rawurlencode($repo) . '/credentials'),
            CredentialListWrapper::class,
        );
        return $wrapper->credentials;
    }

    public function getCredential(string $repo, string $credentialId): Credential
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'GET',
                'repos/' . rawurlencode($repo) . '/credentials/' . rawurlencode($credentialId),
            ),
            Credential::class,
        );
    }

    public function createCredential(string $repo, NewCredential $request): CreatedCredential
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                'repos/' . rawurlencode($repo) . '/credentials',
                self::JSON_HEADERS,
                Json::encodeArray($request->toRequestBody()),
            ),
            CreatedCredential::class,
        );
    }

    public function deleteCredential(string $repo, string $credentialId): void
    {
        $this->apiClient->sendRequest(
            new Request(
                'DELETE',
                'repos/' . rawurlencode($repo) . '/credentials/' . rawurlencode($credentialId),
            ),
        );
    }
}
