<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Staging;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Exception\StagingException;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\FileStagingInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\File\Strategy\Local;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class StrategyFactoryTest extends TestCase
{
    public function testAccessors(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN'),
            ),
        );
        $logger = new NullLogger();
        $factory = new StrategyFactory($clientWrapper, $logger, 'json');
        self::assertSame($clientWrapper, $factory->getClientWrapper());
        self::assertSame($logger, $factory->getLogger());
        self::assertEquals(
            ['local', 'workspace-snowflake', 'workspace-bigquery'],
            array_keys($factory->getStrategyMap()),
        );
    }

    public function testGetFileStrategyFail(): void
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions(
                    (string) getenv('STORAGE_API_URL'),
                    (string) getenv('STORAGE_API_TOKEN'),
                ),
            ),
            new NullLogger(),
            'json',
        );
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage('The project does not support "local" file output backend.');
        $factory->getFileOutputStrategy(AbstractStrategyFactory::LOCAL);
    }

    public function testGetFileStrategySuccess(): void
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions(
                    (string) getenv('STORAGE_API_URL'),
                    (string) getenv('STORAGE_API_TOKEN'),
                ),
            ),
            new NullLogger(),
            'json',
        );
        $factory->addProvider(
            $this->createMock(FileStagingInterface::class),
            [AbstractStrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA])],
        );
        self::assertInstanceOf(
            Local::class,
            $factory->getFileOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
    }

    public function testGetTableStrategyFail(): void
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions(
                    (string) getenv('STORAGE_API_URL'),
                    (string) getenv('STORAGE_API_TOKEN'),
                ),
            ),
            new NullLogger(),
            'json',
        );
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage('The project does not support "local" table output backend.');
        $factory->getTableOutputStrategy(AbstractStrategyFactory::LOCAL);
    }

    public function testGetTableStrategySuccess(): void
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions(
                    (string) getenv('STORAGE_API_URL'),
                    (string) getenv('STORAGE_API_TOKEN'),
                ),
            ),
            new NullLogger(),
            'json',
        );
        $factory->addProvider(
            $this->createMock(FileStagingInterface::class),
            [AbstractStrategyFactory::LOCAL => new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA])],
        );
        self::assertInstanceOf(
            LocalTableStrategy::class,
            $factory->getTableOutputStrategy(AbstractStrategyFactory::LOCAL),
        );
    }

    public function testAddProviderInvalidStaging(): void
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions(
                    (string) getenv('STORAGE_API_URL'),
                    (string) getenv('STORAGE_API_TOKEN'),
                ),
            ),
            new NullLogger(),
            'json',
        );
        self::expectException(StagingException::class);
        self::expectExceptionMessage('Staging "0" is unknown. Known types are "local, ');
        $factory->addProvider(
            $this->createMock(FileStagingInterface::class),
            [new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA])],
        );
    }

    public function testGetTableStrategyInvalid(): void
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions(
                    (string) getenv('STORAGE_API_URL'),
                    (string) getenv('STORAGE_API_TOKEN'),
                ),
            ),
            new NullLogger(),
            'json',
        );
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('Input mapping on type "invalid" is not supported. Supported types are "local,');
        $factory->getTableOutputStrategy('invalid');
    }
}
