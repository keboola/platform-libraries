<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use Keboola\AzureApiClient\ApiClientFactory\AegSasKeyAuthorizationHeaderResolver;
use Keboola\AzureApiClient\ApiClientFactory\AuthorizationHeaderResolverInterface;

class StaticTokenCredentialsAuthenticator implements AuthenticatorInterface
{
    public function __construct(private readonly string $token)
    {
    }

    public function getAuthenticationToken(string $resource): TokenInterface
    {
        return new StaticToken($this->token);
    }

    public function getHeaderResolver(string $resource): AuthorizationHeaderResolverInterface
    {
        return new AegSasKeyAuthorizationHeaderResolver($this, $resource);
    }
}
