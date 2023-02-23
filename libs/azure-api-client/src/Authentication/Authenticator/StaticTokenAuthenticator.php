<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Keboola\AzureApiClient\Authentication\AegSasKeyAuthorizationHeaderResolver;
use Keboola\AzureApiClient\Authentication\AuthenticationToken;
use Keboola\AzureApiClient\Authentication\AuthorizationHeaderResolverInterface;

class StaticTokenAuthenticator implements AuthenticatorInterface
{
    /**
     * @param class-string<AuthorizationHeaderResolverInterface> $headerResolver
     */
    public function __construct(
        private readonly string $value,
        private readonly string $headerResolver = AegSasKeyAuthorizationHeaderResolver::class
    ) {
    }

    public function getAuthenticationToken(string $resource): AuthenticationToken
    {
        return new AuthenticationToken($this->value, null);
    }

    public function getHeaderResolver(string $resource): AuthorizationHeaderResolverInterface
    {
        return new $this->headerResolver($this, $resource);
    }
}
