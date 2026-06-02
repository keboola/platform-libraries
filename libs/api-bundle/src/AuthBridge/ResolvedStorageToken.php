<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\AuthBridge;

use Keboola\ApiBundle\AuthBridge\Exception\StorageTokenResolverException;
use SensitiveParameter;

/**
 * Immutable result of resolving a Connection programmatic token to a legacy Storage token.
 *
 * Mirrors the response of POST /manage/internal/auth-bridge/resolve-storage-token.
 */
final readonly class ResolvedStorageToken
{
    public function __construct(
        #[SensitiveParameter]
        public string $storageToken,
        public int $projectId,
        public string $tokenId,
        public string $userId,
        public ?string $expiresAt,
    ) {
    }

    /**
     * @throws StorageTokenResolverException
     */
    public static function fromResponseData(array $data): self
    {
        $storageToken = $data['storageToken'] ?? null;
        $projectId = $data['projectId'] ?? null;
        $tokenId = $data['tokenId'] ?? null;
        $userId = $data['userId'] ?? null;
        $expiresAt = $data['expiresAt'] ?? null;

        if (!is_string($storageToken) || $storageToken === '') {
            throw new StorageTokenResolverException('Resolver response is missing "storageToken".');
        }
        if (!is_int($projectId)) {
            throw new StorageTokenResolverException('Resolver response is missing valid "projectId".');
        }
        if (!is_scalar($tokenId)) {
            throw new StorageTokenResolverException('Resolver response is missing valid "tokenId".');
        }
        if (!is_scalar($userId)) {
            throw new StorageTokenResolverException('Resolver response is missing valid "userId".');
        }
        if ($expiresAt !== null && !is_string($expiresAt)) {
            throw new StorageTokenResolverException('Resolver response contains invalid "expiresAt".');
        }

        return new self(
            storageToken: $storageToken,
            projectId: $projectId,
            tokenId: (string) $tokenId,
            userId: (string) $userId,
            expiresAt: $expiresAt,
        );
    }
}
