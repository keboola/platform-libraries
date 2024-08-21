<?php

declare(strict_types=1);

namespace Keboola\DoctrineRetryBundle\Database\Retry;

use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\Middleware as MiddlewareInterface;
use Retry\RetryProxyInterface;

class Middleware implements MiddlewareInterface
{
    private RetryProxyInterface $retryProxy;

    public function __construct(RetryProxyInterface $retryProxy)
    {
        $this->retryProxy = $retryProxy;
    }

    public function wrap(DriverInterface $driver): DriverInterface
    {
        return new Driver($driver, $this->retryProxy);
    }
}
