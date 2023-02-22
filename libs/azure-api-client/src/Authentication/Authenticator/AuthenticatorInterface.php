<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator;

use Keboola\AzureApiClient\Authentication\AuthenticationToken;

interface AuthenticatorInterface
{
    public function getAuthenticationToken(string $resource): AuthenticationToken;
}
