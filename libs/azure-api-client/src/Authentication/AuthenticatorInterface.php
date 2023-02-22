<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

interface AuthenticatorInterface
{
    public function getAuthenticationToken(string $resource): TokenResponse;

    public function checkUsability(): void;
}
