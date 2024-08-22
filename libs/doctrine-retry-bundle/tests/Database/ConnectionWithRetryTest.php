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
            (string) getenv('TEST_DATABASE_URL'),
            $this->logsHandler,
        );
    }

    public function testConnectWithoutRetry(): void
    {
        $return = $this->dbConnection->connect();

        self::assertTrue($return);

        self::assertCount(1, $this->logsHandler->getRecords());
        self::assertTrue($this->logsHandler->hasInfo('Connecting with parameters {params}'));
    }

    public function testConnectWithRetry(): void
    {
        $this->startFailingMysql(1);

        $return = $this->dbConnection->connect();

        self::assertTrue($return);

        self::assertCount(2, $this->logsHandler->getRecords());
        self::assertTrue($this->logsHandler->hasInfo('Connecting with parameters {params}'));
        self::assertTrue($this->logsHandler->hasInfo('SQLSTATE[HY000] [2002] Connection refused. Retrying... [1x]'));
    }

    public function testConnectWithTooManyRetries(): void
    {
        $this->startFailingMysql(self::$MAX_DB_RETRIES + 1);

        try {
            $this->dbConnection->connect();
            $this->fail('Connect should fail');
        } catch (DbalException $e) {
            $this->assertSame(
                'An exception occurred in the driver: SQLSTATE[HY000] [2002] Connection refused',
                $e->getMessage(),
            );
        }

        self::assertCount(3, $this->logsHandler->getRecords());
        self::assertTrue($this->logsHandler->hasInfo('Connecting with parameters {params}'));
        self::assertTrue($this->logsHandler->hasInfo('SQLSTATE[HY000] [2002] Connection refused. Retrying... [1x]'));
        self::assertTrue($this->logsHandler->hasInfo('SQLSTATE[HY000] [2002] Connection refused. Retrying... [2x]'));
    }
}
