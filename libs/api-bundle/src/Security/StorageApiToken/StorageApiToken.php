<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\ApiBundle\Security\TokenInterface;
use Keboola\StorageApiBranch\StorageApiToken as BaseStorageApiToken;

/**
 * Adds Symfony {@see \Symfony\Component\Security\Core\User\UserInterface} semantics to the base
 * token. The auth type ({@see BaseStorageApiToken::getTokenType()}) lives on the base; this class
 * does not override the constructor, so callers pass it (or accept the base's deprecation default).
 */
class StorageApiToken extends BaseStorageApiToken implements TokenInterface
{
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
