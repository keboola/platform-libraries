<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Keboola\AzureApiClient\Authentication\AuthenticationToken;
use Keboola\AzureApiClient\Authentication\AuthorizationHeaderResolverInterface;

interface AuthenticatorInterface
{
    public function getAuthenticationToken(string $resource): AuthenticationToken;

    public function getHeaderResolver(string $resource): AuthorizationHeaderResolverInterface;
}
