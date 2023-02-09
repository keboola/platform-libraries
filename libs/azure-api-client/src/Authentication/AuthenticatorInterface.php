<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

interface AuthenticatorInterface
{
    /**
     * @return non-empty-string
     */
    public function getAuthenticationToken(string $resource): string;

    public function checkUsability(): void;
}
