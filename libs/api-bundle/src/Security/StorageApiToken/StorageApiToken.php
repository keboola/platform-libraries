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
        AuthType $tokenType,
    ) {
        // Auth type stays mandatory here (unlike the deprecated-optional base constructor) so every
        // api-bundle construction states it explicitly; forwarding it to the base means getTokenType()
        // is inherited and no deprecation is triggered.
        parent::__construct($tokenInfo, $tokenValue, $tokenType);
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
