<?php

declare(strict_types=1);

namespace Keboola\ApiClientBase;

use Closure;
use GuzzleHttp\HandlerStack;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class ApiClientOptions
{
    /**
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        public readonly string $userAgent = 'Keboola PHP API Client',
        public readonly int $backoffMaxTries = 5,
        public readonly int $connectTimeout = 10,
        public readonly int $requestTimeout = 120,
        public readonly null|Closure|HandlerStack $requestHandler = null,
        public readonly LoggerInterface $logger = new NullLogger(),
    ) {
    }
}
