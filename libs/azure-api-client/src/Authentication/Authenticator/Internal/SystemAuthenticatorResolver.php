<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator\Internal;

use Keboola\AzureApiClient\ApiClientConfiguration;
use Keboola\AzureApiClient\Authentication\AuthenticationToken;
use Keboola\AzureApiClient\Authentication\Authenticator\ClientCredentialsAuth;
use Keboola\AzureApiClient\Authentication\Authenticator\ManagedCredentialsAuth;

class SystemAuthenticatorResolver implements BearerTokenResolver
{
    private readonly ApiClientConfiguration $configuration;
    private ?BearerTokenResolver $tokenResolver = null;

    public function __construct(
        ?ApiClientConfiguration $configuration = null,
    ) {
        $this->configuration = $configuration ?? new ApiClientConfiguration();
    }

    public function getAuthenticationToken(string $resource): AuthenticationToken
    {
        $this->tokenResolver ??= $this->resolveTokenResolver();
        return $this->tokenResolver->getAuthenticationToken($resource);
    }

    private function resolveTokenResolver(): BearerTokenResolver
    {
        $tenantId = (string) getenv('AZURE_TENANT_ID');
        $clientId = (string) getenv('AZURE_CLIENT_ID');
        $clientSecret = (string) getenv('AZURE_CLIENT_SECRET');
        if ($tenantId !== '' && $clientId !== '' && $clientSecret !== '') {
            $this->configuration->logger->debug(
                'Found Azure client credentials in ENV, using ClientCredentialsAuthenticator'
            );

            return new ClientCredentialsAuth(
                $tenantId,
                $clientId,
                $clientSecret,
                $this->configuration,
            );
        }

        $this->configuration->logger->debug(
            'Azure client credentials not found in ENV, using ManagedCredentialsAuthenticator'
        );
        return new ManagedCredentialsAuth($this->configuration);
    }
}
