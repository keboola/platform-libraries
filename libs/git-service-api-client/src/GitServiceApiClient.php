<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

use GuzzleHttp\Psr7\Request;
use Keboola\GitServiceApiClient\Model\CreatedCredential;
use Keboola\GitServiceApiClient\Model\Credential;
use Keboola\GitServiceApiClient\Model\CredentialListWrapper;
use Keboola\GitServiceApiClient\Model\Repository;

class GitServiceApiClient
{
    private const JSON_HEADERS = ['Content-Type' => 'application/json'];

    private ApiClient $apiClient;

    /**
     * @param non-empty-string $baseUrl
     *
     * Authentication and all other client options come from
     * {@see ApiClientConfiguration}. See {@see ApiClient::__construct()}.
     */
    public function __construct(
        string $baseUrl,
        ?ApiClientConfiguration $configuration = null,
    ) {
        $this->apiClient = new ApiClient($baseUrl, $configuration);
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
