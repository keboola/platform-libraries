<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

final class StaticToken implements TokenInterface
{
    public function __construct(
        /** @var non-empty-string $accessToken */
        private readonly string $accessToken,
    ) {
    }

    public function getToken(): string
    {
        return $this->accessToken;
    }

    public function isValid(): bool
    {
        return true;
    }
}
