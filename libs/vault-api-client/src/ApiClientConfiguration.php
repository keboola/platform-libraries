<?php

declare(strict_types=1);

namespace Keboola\VaultApiClient;

use Closure;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Webmozart\Assert\Assert;

class ApiClientConfiguration
{
    private const DEFAULT_BACKOFF_RETRIES = 10;

    /**
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        public readonly ?string $userAgent = null,
        public readonly int $backoffMaxTries = self::DEFAULT_BACKOFF_RETRIES,
        public readonly null|Closure $requestHandler = null,
        public readonly LoggerInterface $logger = new NullLogger(),
    ) {
        Assert::greaterThanEq($this->backoffMaxTries, 0, 'Backoff max tries must be greater than or equal to 0');
    }
}
