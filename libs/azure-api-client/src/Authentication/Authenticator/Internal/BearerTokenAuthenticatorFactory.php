<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator\Internal;

use Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorFactoryInterface;
use Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorInterface;

class BearerTokenAuthenticatorFactory implements RequestAuthenticatorFactoryInterface
{
    public function __construct(
        private readonly BearerTokenResolver $tokenResolver,
    ) {
    }

    public function createRequestAuthenticator(string $resource): RequestAuthenticatorInterface
    {
        return new BearerTokenAuthenticator($this->tokenResolver, $resource);
    }
}
