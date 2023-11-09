<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\StorageApiToken;

use Keboola\ApiBundle\Security\TokenInterface;
use Keboola\StorageApiBranch\StorageApiToken as BaseStorageApiToken;

class StorageApiToken extends BaseStorageApiToken implements TokenInterface
{
    public function eraseCredentials(): void
    {
    }

    public function getUserIdentifier(): string
    {
        return $this->getTokenId();
    }
}
