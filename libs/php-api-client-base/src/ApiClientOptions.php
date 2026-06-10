<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use Closure;
use GuzzleHttp\HandlerStack;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ApiClientOptions
{
    public const DEFAULT_BACKOFF_MAX_TRIES = 5;
    public const DEFAULT_CONNECT_TIMEOUT = 10;
    public const DEFAULT_REQUEST_TIMEOUT = 120;

    /**
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        public readonly string $userAgent = 'Keboola PHP API Client',
        public readonly int $backoffMaxTries = self::DEFAULT_BACKOFF_MAX_TRIES,
        public readonly int $connectTimeout = self::DEFAULT_CONNECT_TIMEOUT,
        public readonly int $requestTimeout = self::DEFAULT_REQUEST_TIMEOUT,
        public readonly null|Closure|HandlerStack $requestHandler = null,
        public readonly LoggerInterface $logger = new NullLogger(),
    ) {
    }
}
