<?php

declare(strict_types=1);

namespace Keboola\DoctrineRetryBundle\Tests;

use Retry\RetryProxyInterface;

/**
 * RetryProxy wrapper that allows putting a callback before each call. The callback is supposed to enable/disable
 * some low-level functionality (for example, a network proxy to external service) to simulate service failures.
 */
class FailingRetryProxy implements RetryProxyInterface
{
    private RetryProxyInterface $innerProxy;

    /** @var callable(bool $shouldFail): void */
    private $failCall;

    private int $failingTries = 0;

    public function __construct(RetryProxyInterface $innerProxy, callable $failCall)
    {
        $this->innerProxy = $innerProxy;
        $this->failCall = $failCall;
    }

    public function startFailing(int $tries = PHP_INT_MAX): void
    {
        $this->failingTries = $tries;
    }

    /**
     * {@inheritdoc}
     */
    public function call(callable $action, array $arguments = [])
    {
        return $this->innerProxy->call(function () use ($action, $arguments) {
            $shouldFail = $this->failingTries > 0;
            $this->failingTries = max(0, $this->failingTries - 1);

            $beforeCall = $this->failCall;
            $beforeCall($shouldFail);

            return $action(...$arguments);
        });
    }

    /**
     * {@inheritdoc}
     */
    public function getTryCount(): int
    {
        return $this->innerProxy->getTryCount();
    }
}
