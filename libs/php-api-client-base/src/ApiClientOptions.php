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
     * @param list<int> $retryableStatusCodes Non-5xx status codes to also retry (e.g. [429]).
     * @param (Closure(string, int): ?string)|null $errorMessageResolver
     *   Maps a (responseBody, statusCode) to an error message, or null to fall back to the default.
     */
    public function __construct(
        public readonly string $userAgent = 'Keboola PHP API Client',
        public readonly int $backoffMaxTries = 5,
        public readonly array $retryableStatusCodes = [],
        public readonly int $connectTimeout = 10,
        public readonly int $requestTimeout = 120,
        public readonly null|Closure|HandlerStack $requestHandler = null,
        public readonly LoggerInterface $logger = new NullLogger(),
        public readonly ?Closure $errorMessageResolver = null,
    ) {
    }
}
