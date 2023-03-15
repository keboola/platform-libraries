<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication\Authenticator\Internal;

use Keboola\AzureApiClient\Authentication\AuthenticationToken;

interface BearerTokenResolver
{
    public function getAuthenticationToken(string $resource): AuthenticationToken;
}
