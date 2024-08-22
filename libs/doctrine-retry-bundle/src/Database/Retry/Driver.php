<?php

declare(strict_types=1);

namespace Keboola\DoctrineRetryBundle\Database\Retry;

use Doctrine\DBAL\Driver as DriverInterface;
use Doctrine\DBAL\Driver\Middleware\AbstractDriverMiddleware;
use Retry\RetryProxyInterface;

class Driver extends AbstractDriverMiddleware
{
    private RetryProxyInterface $retryProxy;

    /**
     * @internal This driver can be only instantiated by its middleware.
     */
    public function __construct(DriverInterface $driver, RetryProxyInterface $retryProxy)
    {
        parent::__construct($driver);

        $this->retryProxy = $retryProxy;
    }

    /**
     * {@inheritDoc}
     */
    public function connect(array $params): DriverInterface\Connection
    {
        /** @var DriverInterface\Connection $result */
        $result = $this->retryProxy->call(fn() => parent::connect($params));
        return $result;
    }
}
