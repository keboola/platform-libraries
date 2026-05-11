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

    public function createRepository(string $appId): Repository
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                'repos',
                self::JSON_HEADERS,
                Json::encodeArray(['name' => $appId]),
            ),
            Repository::class,
        );
    }

    public function getRepository(string $appId): Repository
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request('GET', 'repos/' . rawurlencode($appId)),
            Repository::class,
        );
    }

    public function deleteRepository(string $appId): void
    {
        $this->apiClient->sendRequest(
            new Request('DELETE', 'repos/' . rawurlencode($appId)),
        );
    }

    /**
     * @return list<DeployKey>
     */
    public function listKeys(string $appId): array
    {
        $wrapper = $this->apiClient->sendRequestAndMapResponse(
            new Request('GET', 'repos/' . rawurlencode($appId) . '/keys'),
            KeyListWrapper::class,
        );
        return $wrapper->keys;
    }

    public function getKey(string $appId, string $keyId): DeployKey
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'GET',
                'repos/' . rawurlencode($appId) . '/keys/' . rawurlencode($keyId),
            ),
            DeployKey::class,
        );
    }

    public function addKey(string $appId, string $publicKey, KeyPermission $permissions): DeployKey
    {
        return $this->apiClient->sendRequestAndMapResponse(
            new Request(
                'POST',
                'repos/' . rawurlencode($appId) . '/keys',
                self::JSON_HEADERS,
                Json::encodeArray([
                    'publicKey' => $publicKey,
                    'permissions' => $permissions->value,
                ]),
            ),
            DeployKey::class,
        );
    }

    public function deleteKey(string $appId, string $keyId): void
    {
        $this->apiClient->sendRequest(
            new Request(
                'DELETE',
                'repos/' . rawurlencode($appId) . '/keys/' . rawurlencode($keyId),
            ),
        );
    }
}
