<?php

declare(strict_types=1);

namespace Keboola\GitServiceApiClient;

use Closure;
use GuzzleHttp\HandlerStack;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ApiClientConfiguration
{
    private const DEFAULT_BACKOFF_RETRIES = 5;

    /**
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        public readonly ?string $userAgent = null,
        public readonly int $backoffMaxTries = self::DEFAULT_BACKOFF_RETRIES,
        public readonly null|Closure|HandlerStack $requestHandler = null,
        public readonly LoggerInterface $logger = new NullLogger(),
    ) {
    }
}
