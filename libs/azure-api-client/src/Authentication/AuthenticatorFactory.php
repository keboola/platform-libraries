<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use Keboola\AzureApiClient\Exception\ClientException;
use Keboola\AzureApiClient\GuzzleClientFactory;

class AuthenticatorFactory
{
    public function getAuthenticator(GuzzleClientFactory $clientFactory): AuthenticatorInterface
    {
        $authenticator = new ClientCredentialsEnvironmentAuthenticator($clientFactory);
        try {
            $authenticator->checkUsability();
            return $authenticator;
        } catch (ClientException $e) {
            $clientFactory->getLogger()->debug(
                'ClientCredentialsEnvironmentAuthenticator is not usable: ' . $e->getMessage()
            );
        }
        /* ManagedCredentialsAuthenticator checkUsability method has poor performance due to slow responses
            from GET /metadata requests */
        return new ManagedCredentialsAuthenticator($clientFactory);
    }
}
