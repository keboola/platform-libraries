<?php

declare(strict_types=1);

namespace Keboola\DoctrineRetryBundle\Tests\Database;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use Keboola\DoctrineRetryBundle\Tests\MysqlProxyTestTrait;
use Monolog\Handler\TestHandler;
use PHPUnit\Framework\TestCase;

class ConnectionWithRetryTest extends TestCase
{
    use MysqlProxyTestTrait;

    private TestHandler $logsHandler;
    private Connection $dbConnection;

    protected function setUp(): void
    {
        parent::setUp();

        $this->logsHandler = new TestHandler();
        $this->dbConnection = $this->setUpMysqlProxy(
            (string) getenv('TEST_DATABASE_HOST'),
            (int) getenv('TEST_DATABASE_PORT'),
            (string) getenv('TEST_DATABASE_USER'),
            (string) getenv('TEST_DATABASE_PASSWORD'),
            (string) getenv('TEST_DATABASE_DB'),
            (string) getenv('TEST_PROXY_HOST'),
            $this->logsHandler,
        );
    }

    public function testConnectWithoutRetry(): void
    {
        $return = $this->dbConnection->executeQuery('SELECT 1')->fetchFirstColumn();

        self::assertEquals([1], $return);

        self::assertTrue($this->logsHandler->hasInfoThatContains('Connecting with parameters {params}'));
    }

    public function testConnectWithRetry(): void
    {
        $this->startFailingMysql(1);

        $return = $this->dbConnection->executeQuery('SELECT 1')->fetchFirstColumn();

        self::assertEquals([1], $return);

        self::assertTrue($this->logsHandler->hasInfoThatContains('Connecting with parameters {params}'));
        self::assertTrue($this->logsHandler->hasInfoThatContains(
            'SQLSTATE[HY000] [2002] Connection refused. Retrying... [1x]',
        ));
    }

    public function testConnectWithTooManyRetries(): void
    {
        $this->startFailingMysql(self::$MAX_DB_RETRIES + 1);

        try {
            $this->dbConnection->executeQuery('SELECT 1')->fetchFirstColumn();
            self::fail('Connect should fail');
        } catch (DbalException $e) {
            self::assertSame(
                'An exception occurred in the driver: SQLSTATE[HY000] [2002] Connection refused',
                $e->getMessage(),
            );
        }

        self::assertTrue($this->logsHandler->hasInfoThatContains('Connecting with parameters {params}'));
        self::assertTrue($this->logsHandler->hasInfoThatContains(
            'SQLSTATE[HY000] [2002] Connection refused. Retrying... [1x]',
        ));
        self::assertTrue($this->logsHandler->hasInfoThatContains(
            'SQLSTATE[HY000] [2002] Connection refused. Retrying... [2x]',
        ));
    }
}
