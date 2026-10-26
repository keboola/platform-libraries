<?php

declare(strict_types=1);

namespace Keboola\AzureApiClient;

use Closure;
use Keboola\AzureApiClient\Authentication\Authenticator\Internal\BearerTokenResolver;
use Keboola\AzureApiClient\Authentication\Authenticator\RequestAuthenticatorFactoryInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Webmozart\Assert\Assert;

class ApiClientConfiguration
{
    private const DEFAULT_BACKOFF_RETRIES = 10;
    private const DEFAULT_CONNECT_TIMEOUT_SECONDS = 10;
    private const DEFAULT_REQUEST_TIMEOUT_SECONDS = 120;

    /**
     * @param int<0, max> $backoffMaxTries
     * @param positive-int $connectTimeout Seconds to wait for the TCP/TLS connection.
     * @param positive-int $requestTimeout Seconds the whole request may take, including a long-polling wait.
     */
    public function __construct(
        public readonly int $backoffMaxTries = self::DEFAULT_BACKOFF_RETRIES,
        public readonly null|RequestAuthenticatorFactoryInterface|BearerTokenResolver $authenticator = null,
        public readonly null|Closure $requestHandler = null,
        public readonly LoggerInterface $logger = new NullLogger(),
        public readonly int $connectTimeout = self::DEFAULT_CONNECT_TIMEOUT_SECONDS,
        public readonly int $requestTimeout = self::DEFAULT_REQUEST_TIMEOUT_SECONDS,
    ) {
        Assert::greaterThanEq($this->backoffMaxTries, 0);
        Assert::greaterThan($this->connectTimeout, 0);
        Assert::greaterThan($this->requestTimeout, 0);
    }
}
