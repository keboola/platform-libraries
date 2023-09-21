<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Staging;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Exception\StagingException;
use Keboola\InputMapping\File\Strategy\Local as LocalFile;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\Local as LocalTable;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class StrategyFactoryTest extends TestCase
{
    public function testAccessors(): void
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
        );
        $logger = new NullLogger();
        $factory = new StrategyFactory($clientWrapper, $logger, 'json');
        self::assertSame($clientWrapper, $factory->getClientWrapper());
        self::assertSame($logger, $factory->getLogger());
        self::assertEquals(
            ['abs', 'local', 's3', 'workspace-abs', 'workspace-redshift',
                'workspace-snowflake', 'workspace-synapse', 'workspace-exasol', 'workspace-teradata',
                'workspace-bigquery',
            ],
            array_keys($factory->getStrategyMap()),
        );
    }

    public function testGetFileStrategyFail(): void
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
            ),
            new NullLogger(),
            'json',
        );
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage('The project does not support "local" file input backend.');
        $factory->getFileInputStrategy(
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([]),
        );
    }

    public function testGetFileStrategySuccess(): void
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
            ),
            new NullLogger(),
            'json',
        );
        $factory->addProvider(
            new NullProvider(),
            [AbstractStrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA])],
        );
        self::assertInstanceOf(
            LocalFile::class,
            $factory->getFileInputStrategy(
                AbstractStrategyFactory::LOCAL,
                new InputFileStateList([]),
            ),
        );
    }

    public function testGetTableStrategyFail(): void
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
            ),
            new NullLogger(),
            'json',
        );
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage('The project does not support "local" table input backend.');
        $factory->getTableInputStrategy(AbstractStrategyFactory::LOCAL, 'test', new InputTableStateList([]));
    }

    public function testGetTableStrategySuccess(): void
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
            ),
            new NullLogger(),
            'json',
        );
        $factory->addProvider(
            new NullProvider(),
            [AbstractStrategyFactory::LOCAL => new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA])],
        );
        self::assertInstanceOf(
            LocalTable::class,
            $factory->getTableInputStrategy(AbstractStrategyFactory::LOCAL, 'test', new InputTableStateList([])),
        );
    }

    public function testAddProviderInvalidStaging(): void
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
            ),
            new NullLogger(),
            'json',
        );
        $this->expectException(StagingException::class);
        $this->expectExceptionMessage(
            'Staging "0" is unknown. Known types are "abs, local, s3, workspace-abs, ' .
            'workspace-redshift, workspace-snowflake, workspace-synapse, workspace-exasol, workspace-teradata, ' .
            'workspace-bigquery',
        );
        $factory->addProvider(new NullProvider(), [new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA])]);
    }

    public function testGetTableStrategyInvalid(): void
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions((string) getenv('STORAGE_API_URL'), (string) getenv('STORAGE_API_TOKEN')),
            ),
            new NullLogger(),
            'json',
        );
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(
            'Input mapping on type "invalid" is not supported. Supported types are "abs, local,',
        );
        $factory->getTableInputStrategy('invalid', 'test', new InputTableStateList([]));
    }
}
