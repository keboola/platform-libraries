<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient\Authentication;

use DateTimeImmutable;

class AuthenticationToken
{
    public function __construct(
        public readonly string $value,
        public readonly ?DateTimeImmutable $expiresAt,
    ) {
    }
}
