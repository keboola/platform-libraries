<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ManageToken;

use Keboola\ManageApi\Client;

class ManageApiClientFactory
{
    public function __construct(
        private readonly string $appName,
        private readonly string $storageApiUrl
    ) {
    }

    public function getClient(string $token): Client
    {
        return new Client([
            'url' => $this->storageApiUrl,
            'token' => $token,
            'userAgent' => $this->appName,
        ]);
    }
}
