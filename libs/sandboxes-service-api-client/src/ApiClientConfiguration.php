<?php

declare(strict_types=1);

namespace Keboola\SandboxesServiceApiClient;

use Closure;
use Keboola\SandboxesServiceApiClient\Authentication\StorageTokenAuthenticator;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Webmozart\Assert\Assert;

class ApiClientConfiguration
{
    private const DEFAULT_BACKOFF_RETRIES = 10;

    private const DEFAULT_USER_AGENT = 'Keboola Sandboxes Service API PHP Client';

    /**
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        public readonly string $userAgent = self::DEFAULT_USER_AGENT,
        public readonly int $backoffMaxTries = self::DEFAULT_BACKOFF_RETRIES,
        public readonly null|StorageTokenAuthenticator $authenticator = null,
        public readonly null|Closure $requestHandler = null,
        public readonly LoggerInterface $logger = new NullLogger(),
    ) {
        Assert::greaterThanEq($this->backoffMaxTries, 0);
    }
}