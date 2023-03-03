<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\AuthenticationToken;

class SystemAuthenticatorResolver implements AuthenticatorInterface
{
    private readonly ApiClientConfiguration $configuration;
    private ?AuthenticatorInterface $resolvedAuthenticator = null;

    public function __construct(
        ?ApiClientConfiguration $configuration = null,
    ) {
        $this->configuration = $configuration ?? new ApiClientConfiguration();
    }

    public function getAuthenticationToken(string $resource): AuthenticationToken
    {
        $this->resolvedAuthenticator ??= $this->resolveAuthenticator();
        return $this->resolvedAuthenticator->getAuthenticationToken($resource);
    }

    private function resolveAuthenticator(): AuthenticatorInterface
    {
        $tenantId = (string) getenv('AZURE_TENANT_ID');
        $clientId = (string) getenv('AZURE_CLIENT_ID');
        $clientSecret = (string) getenv('AZURE_CLIENT_SECRET');
        if ($tenantId !== '' && $clientId !== '' && $clientSecret !== '') {
            $this->configuration->logger->debug(
                'Found Azure client credentials in ENV, using ClientCredentialsAuthenticator'
            );

            return new ClientCredentialsAuthenticator(
                $tenantId,
                $clientId,
                $clientSecret,
                $this->configuration,
            );
        }

        $this->configuration->logger->debug(
            'Azure client credentials not found in ENV, using ManagedCredentialsAuthenticator'
        );
        return new ManagedCredentialsAuthenticator($this->configuration);
    }
}
