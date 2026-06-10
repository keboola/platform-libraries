<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase\Auth;

final readonly class NoAuthAuthenticator implements RequestAuthenticatorInterface
{
    public function getAuthenticationHeaders(): array
    {
        return [];
    }
}
