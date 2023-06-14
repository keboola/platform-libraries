<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ManageToken;

use Keboola\ManageApi\Client as ManageApiClient;

class ManageApiClientFactory
{
    public function __construct(
        private readonly string $appName,
        private readonly string $storageApiUrl
    ) {
    }

    public function getClient(string $token): ManageApiClient
    {
        return new ManageApiClient([
            'url' => $this->storageApiUrl,
            'token' => $token,
            'userAgent' => $this->appName,
        ]);
    }
}
