<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

class StaticTokenCredentialsAuthenticator implements AuthenticatorInterface
{
    public function __construct(private readonly string $token)
    {
    }

    public function getAuthenticationToken(string $resource): string
    {
        return $this->token;
    }

    public function checkUsability(): void
    {
    }
}
