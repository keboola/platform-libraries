<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\FixtureTraits;

trait StorageTokenTrait
{
    private string $storageToken;

    public function setStorageToken(string $storageToken): void
    {
        $this->storageToken = $storageToken;
    }

    public function getStorageToken(): string
    {
        return $this->storageToken;
    }
}
