<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

interface RequestAuthenticatorFactoryInterface
{
    public function createRequestAuthenticator(string $resource): RequestAuthenticatorInterface;
}
