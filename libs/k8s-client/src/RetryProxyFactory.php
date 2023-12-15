<?php

declare(strict_types=1);

namespace Keboola\K8sClient;

use Psr\Log\LoggerInterface;
use Retry\BackOff\ExponentialBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

class RetryProxyFactory
{
    // total 31000ms max waiting time
    private const KUBERNETES_MAX_RETRIES = 6;
    private const KUBERNETES_INITIAL_DELAY_MS = 1000;
    private const KUBERNETES_MAX_DELAY_MS = 8000;

    private const RETRY_MULTIPLIER = 2.0;

    private LoggerInterface $logger;

    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function createRetryProxy(): RetryProxy
    {
        return new RetryProxy(
            new SimpleRetryPolicy(self::KUBERNETES_MAX_RETRIES),
            new ExponentialBackOffPolicy(
                self::KUBERNETES_INITIAL_DELAY_MS,
                self::RETRY_MULTIPLIER,
                self::KUBERNETES_MAX_DELAY_MS,
            ),
            $this->logger,
        );
    }
}
