<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Keboola\AzureApiClient\Authentication\AuthenticationToken;
use Keboola\AzureApiClient\Authentication\Authenticator\Internal\BearerTokenResolver;

class StaticBearerTokenAuth implements BearerTokenResolver
{
    public function __construct(
        private readonly string $token,
    ) {
    }

    public function getAuthenticationToken(string $resource): AuthenticationToken
    {
        return new AuthenticationToken(
            $this->token,
            null,
        );
    }
}
