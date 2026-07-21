<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\StorageApiClient;

use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Symfony\Component\HttpFoundation\Request;

class StorageClientApiFactory
{
    public const RUN_ID_HEADER = RequestStorageClientFactory::RUN_ID_HEADER;

    public function __construct(
        private readonly RequestStorageClientFactory $requestStorageClientFactory,
        private readonly Request $request,
        private readonly StorageApiToken $token,
    ) {
    }

    public function createClientWrapper(?ClientOptions $clientOptions = null): ClientWrapper
    {
        return $this->requestStorageClientFactory->createClientWrapper(
            $this->token->getTokenValue(),
            $this->token->getTokenType(),
            $this->request,
            $clientOptions,
        );
    }
}
