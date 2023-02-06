<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use Keboola\AzureApiClient\ApiClientFactory\PlainAzureApiClientFactory;
use Keboola\AzureApiClient\Exception\ClientException;
use Psr\Log\LoggerInterface;

class AuthenticatorFactory
{
    public function __construct(
        private readonly PlainAzureApiClientFactory $clientFactory,
        private readonly LoggerInterface $logger,
    ) {
    }

    public function createAuthenticator(): AuthenticatorInterface
    {
        $authenticator = new ClientCredentialsEnvironmentAuthenticator($this->clientFactory, $this->logger);
        try {
            $authenticator->checkUsability();
            return $authenticator;
        } catch (ClientException $e) {
            $this->logger->debug(
                'ClientCredentialsEnvironmentAuthenticator is not usable: ' . $e->getMessage()
            );
        }
        /* ManagedCredentialsAuthenticator checkUsability method has poor performance due to slow responses
            from GET /metadata requests */
        return new ManagedCredentialsAuthenticator($this->clientFactory, $this->logger);
    }
}
