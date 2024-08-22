<?php

declare(strict_types=1);

namespace Keboola\DoctrineRetryBundle\Tests;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Logging\Middleware as LoggingMiddleware;
use Ihsw\Toxiproxy\Proxy;
use Ihsw\Toxiproxy\Toxiproxy;
use Keboola\DoctrineRetryBundle\Database\Retry\Middleware as RetryMiddleware;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use Retry\BackOff\NoBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

/**
 * Set-up a DBAL MySQL connection through a Toxiproxy to simulate communication failures between the app and the DB.
 */
trait MysqlProxyTestTrait
{
    protected static int $MAX_DB_RETRIES = 3; // traits can't have constants

    private FailingRetryProxy $mysqlRetryProxy;

    protected function setUpMysqlProxy(string $mysqlUrl, TestHandler $logsHandler): Connection
    {
        $toxiproxy = new Toxiproxy('http://toxiproxy:8474');
        foreach ($toxiproxy->getAll() as $proxy) {
            $toxiproxy->delete($proxy);
        }
        $toxiproxy->reset();

        $realMysqlHostPort = $this->resolveRealMysqlHostPort($mysqlUrl);

        $mysqlProxy = $toxiproxy->create('mysql', $realMysqlHostPort);
        $proxyMysqlHostPort = $this->resolveProxyMysqlHostPort($mysqlUrl, $mysqlProxy);

        $this->mysqlRetryProxy = new FailingRetryProxy(
            new RetryProxy(
                new SimpleRetryPolicy(self::$MAX_DB_RETRIES),
                new NoBackOffPolicy(),
                new Logger('retry', [$logsHandler]),
            ),
            function (bool $shouldFail) use ($toxiproxy, $mysqlProxy): void {
                $mysqlProxy->setEnabled($shouldFail === false);
                $toxiproxy->update($mysqlProxy);
            },
        );

        $configuration = new Configuration();
        $configuration->setMiddlewares([
            new RetryMiddleware($this->mysqlRetryProxy),
            new LoggingMiddleware(new Logger('db', [$logsHandler])),
        ]);

        return DriverManager::getConnection([
            'url' => $proxyMysqlHostPort,
        ], $configuration);
    }

    private function resolveRealMysqlHostPort(string $mysqlUrl): string
    {
        $urlParts = (array) parse_url($mysqlUrl);
        return sprintf('%s:%s', $urlParts['host'] ?? '', $urlParts['port'] ?? '');
    }

    private function resolveProxyMysqlHostPort(string $mysqlUrl, Proxy $mysqlProxy): string
    {
        $realHostPort = $this->resolveRealMysqlHostPort($mysqlUrl);
        $proxyHostPort = sprintf('%s:%s', 'toxiproxy', $mysqlProxy->getListenPort());

        return str_replace($realHostPort, $proxyHostPort, $mysqlUrl);
    }

    /**
     * Make next $failsCount DB requests fail with network connection error.
     */
    protected function startFailingMysql(int $failsCount): void
    {
        $this->mysqlRetryProxy->startFailing($failsCount);
    }
}
