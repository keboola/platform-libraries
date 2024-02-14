<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient;

use Closure;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Webmozart\Assert\Assert;

class ApiClientConfiguration
{
    private const DEFAULT_BACKOFF_RETRIES = 10;

    /**
     * @param non-empty-string $baseUrl
     * @param non-empty-string $storageToken
     * @param non-empty-string $userAgent
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        public readonly string $baseUrl,
        public readonly string $storageToken,
        public readonly string $userAgent,
        public readonly int $backoffMaxTries = self::DEFAULT_BACKOFF_RETRIES,
        public readonly null|Closure $requestHandler = null,
        public readonly LoggerInterface $logger = new NullLogger(),
    ) {
        Assert::stringNotEmpty($this->baseUrl);
        Assert::stringNotEmpty($this->storageToken);
        Assert::stringNotEmpty($this->userAgent);
        Assert::greaterThanEq($this->backoffMaxTries, 0);
    }
}
