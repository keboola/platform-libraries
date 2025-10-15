<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures\FixtureTraits;

use Keboola\ServiceClient\ServiceClient;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;

trait StorageApiAwareTrait
{
    private ClientWrapper $clientWrapper;

    /**
     * @param non-empty-string $hostnameSuffix
     */
    public function createStorageClientWrapper(string $hostnameSuffix, string $token): void
    {
        $storageClientOptions = new ClientOptions(
            url: (new ServiceClient($hostnameSuffix))->getConnectionServiceUrl(),
            token: $token,
        );
        $this->clientWrapper = (new ClientWrapper($storageClientOptions));
    }

    public function getStorageClientWrapper(): ClientWrapper
    {
        return $this->clientWrapper;
    }
}
