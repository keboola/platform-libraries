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

    /**
     * @param int<0, max> $backoffMaxTries
     */
    public function __construct(
        public readonly int $backoffMaxTries = self::DEFAULT_BACKOFF_RETRIES,
        public readonly null|RequestAuthenticatorFactoryInterface|BearerTokenResolver $authenticator = null,
        public readonly null|Closure $requestHandler = null,
        public readonly LoggerInterface $logger = new NullLogger(),
    ) {
        Assert::greaterThanEq($this->backoffMaxTries, 0);
    }
}
