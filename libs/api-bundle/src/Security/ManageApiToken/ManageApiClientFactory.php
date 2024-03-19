<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ManageApiToken;

use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ServiceClient\ServiceClient;

class ManageApiClientFactory
{
    public function __construct(
        private readonly string $appName,
        private readonly ServiceClient $serviceClient,
    ) {
    }

    public function getClient(string $token): ManageApiClient
    {
        return new ManageApiClient([
            'url' => $this->serviceClient->getConnectionServiceUrl(),
            'token' => $token,
            'userAgent' => $this->appName,
        ]);
    }
}
