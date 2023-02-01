<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient;

use Keboola\AzureApiClient\Authentication\AuthenticatorFactory;
use Psr\Log\LoggerInterface;

class AzureApiClientFactory
{
    public function __construct(
        private readonly GuzzleClientFactory $guzzleClientFactory,
        private readonly AuthenticatorFactory $authenticatorFactory,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function getClient(string $baseUrl, string $resource, array $options = []): AzureApiClient
    {
        return new AzureApiClient(
            $baseUrl,
            $resource,
            $this->guzzleClientFactory,
            $this->authenticatorFactory,
            $this->logger,
            $options,
        );
    }
}
