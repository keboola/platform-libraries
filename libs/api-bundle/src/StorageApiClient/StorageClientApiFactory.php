<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\StorageApiClient;

use Keboola\ApiBundle\Security\StorageApiToken\StorageApiToken;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Symfony\Component\HttpFoundation\Request;

class StorageClientApiFactory
{
    public const RUN_ID_HEADER = StorageClientWrapperFactory::RUN_ID_HEADER;

    public function __construct(
        private readonly ClientOptions $baseClientOptions,
        private readonly Request $request,
        private readonly StorageApiToken $token,
    ) {
    }

    public function createClientWrapper(?ClientOptions $clientOptions = null): ClientWrapper
    {
        return StorageClientWrapperFactory::create(
            $this->baseClientOptions,
            $this->token->getTokenValue(),
            $this->token->getTokenType(),
            $this->request,
            $clientOptions,
        );
    }
}
