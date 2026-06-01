<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Security\ApplicationToken;

use Keboola\ManageApi\Client as ManageApiClient;
use Keboola\ServiceClient\ServiceClient;

class ManageApiClientFactory
{
    public function __construct(
        private readonly string $appName,
        private readonly ServiceClient $serviceClient,
    ) {
    }

    public function getClientForManageToken(string $token): ManageApiClient
    {
        return new ManageApiClient([
            'url' => $this->serviceClient->getConnectionServiceUrl(),
            'token' => $token,
            'userAgent' => $this->appName,
        ]);
    }

    public function getClientForServiceAccountToken(string $jwt): ManageApiClient
    {
        return new ManageApiClient([
            'url' => $this->serviceClient->getConnectionServiceUrl(),
            'jwtToken' => $jwt,
            'userAgent' => $this->appName,
        ]);
    }
}
