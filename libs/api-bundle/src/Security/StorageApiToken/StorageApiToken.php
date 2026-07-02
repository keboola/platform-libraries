<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\ApiBundle\Security\TokenInterface;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\StorageApiToken as BaseStorageApiToken;
use SensitiveParameter;

class StorageApiToken extends BaseStorageApiToken implements TokenInterface
{
    /**
     * @param array<mixed, mixed> $tokenInfo
     */
    public function __construct(
        array $tokenInfo,
        #[SensitiveParameter] string $tokenValue,
        private readonly AuthType $tokenType,
    ) {
        parent::__construct($tokenInfo, $tokenValue);
    }

    /**
     * Auth type the token was resolved with: {@see AuthType::STORAGE_TOKEN} for a legacy Storage
     * token (also the exchanged programmatic-token path) or {@see AuthType::BEARER} for an OAuth
     * bearer token. Lets consumers build a Storage client with the matching auth scheme instead of
     * assuming the token value is always a Storage token.
     */
    public function getTokenType(): AuthType
    {
        return $this->tokenType;
    }

    public function eraseCredentials(): void
    {
    }

    public function getUserIdentifier(): string
    {
        /** @var non-empty-string $tokenId */
        $tokenId = $this->getTokenId();
        return $tokenId;
    }
}
