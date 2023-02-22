<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

interface TokenInterface
{
    public const EXPIRATION_MARGIN = 60; // seconds

    public function getToken(): string;

    public function isValid(): bool;
}
