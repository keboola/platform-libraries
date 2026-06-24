<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\StorageApiClient;

use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\StorageApiToken;
use Symfony\Component\HttpFoundation\Request;

class RequestStorageClientFactory
{
    public function __construct(
        private readonly StorageClientApiFactory $factory,
        private readonly Request $request,
        private readonly StorageApiToken $token,
    ) {
    }

    public function createClientWrapper(?ClientOptions $clientOptions = null): ClientWrapper
    {
        return $this->factory->createClientWrapper($this->request, $this->token, $clientOptions);
    }
}
