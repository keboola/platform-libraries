<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use Keboola\AzureApiClient\ApiClientFactory\AuthorizationHeaderResolverInterface;

interface AuthenticatorInterface
{
    public function getAuthenticationToken(string $resource): TokenInterface;

    public function getHeaderResolver(string $resource): AuthorizationHeaderResolverInterface;
}
