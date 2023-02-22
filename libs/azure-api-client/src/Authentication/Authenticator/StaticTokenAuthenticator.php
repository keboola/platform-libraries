<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Keboola\AzureApiClient\Authentication\AuthenticationToken;

class StaticTokenAuthenticator implements AuthenticatorInterface
{
    public function __construct(
        private readonly string $value,
    ) {
    }

    public function getAuthenticationToken(string $resource): AuthenticationToken
    {
        return new AuthenticationToken($this->value, null);
    }
}
