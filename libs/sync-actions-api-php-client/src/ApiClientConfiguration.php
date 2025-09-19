<?php

declare(strict_types=1);

namespace Keboola\SyncActionsClient;

use Closure;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

readonly class ApiClientConfiguration
{
    private const int DEFAULT_BACKOFF_RETRIES = 10;

    /**
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        public ?string $userAgent = null,
        public int $backoffMaxTries = self::DEFAULT_BACKOFF_RETRIES,
        public null|Closure $requestHandler = null,
        public LoggerInterface $logger = new NullLogger(),
    ) {
    }
}
