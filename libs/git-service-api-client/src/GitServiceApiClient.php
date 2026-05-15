<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

use GuzzleHttp\Psr7\Request;
use Keboola\GitServiceApiClient\Model\DeployKey;
use Keboola\GitServiceApiClient\Model\KeyListWrapper;
use Keboola\GitServiceApiClient\Model\Repository;
use SensitiveParameter;

class GitServiceApiClient
{
    private const JSON_HEADERS = ['Content-Type' => 'application/json'];

    private ApiClient $apiClient;

    /**
     * @param non-empty-string $baseUrl
     * @param non-empty-string $token
     */
    public function __construct(
        string $baseUrl,
        #[SensitiveParameter] string $token,
        ?ApiClientConfiguration $configuration = null,
    ) {
        $this->apiClient = new ApiClient($baseUrl, $token, $configuration);
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
     * @return list<DeployKey>
     */
    public function listKeys(string $repo): array
    {
        $wrapper = $this->apiClient->sendRequestAndMapResponse(
            new Request('GET', 'repos/' . rawurlencode($repo) . '/keys'),
            KeyListWrapper::class,
        );
        return $wrapper->keys;
    }

    public function getKey(string $repo, string $keyId): DeployKey
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'GET',
                'repos/' . rawurlencode($repo) . '/keys/' . rawurlencode($keyId),
            ),
            DeployKey::class,
        );
    }

    public function addKey(string $repo, string $publicKey, KeyPermission $permissions): DeployKey
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                'repos/' . rawurlencode($repo) . '/keys',
                self::JSON_HEADERS,
                Json::encodeArray([
                    'publicKey' => $publicKey,
                    'permissions' => $permissions->value,
                ]),
            ),
            DeployKey::class,
        );
    }

    public function deleteKey(string $repo, string $keyId): void
    {
        $this->apiClient->sendRequest(
            new Request(
                'DELETE',
                'repos/' . rawurlencode($repo) . '/keys/' . rawurlencode($keyId),
            ),
        );
    }
}
