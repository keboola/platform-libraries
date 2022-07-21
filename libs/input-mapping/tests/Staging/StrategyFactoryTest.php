<?php

namespace Keboola\InputMapping\Tests\Staging;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Exception\StagingException;
use Keboola\InputMapping\File\Strategy\Local as LocalFile;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\Table\Strategy\Local as LocalTable;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class StrategyFactoryTest extends TestCase
{
    public function testAccessors()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN),
        );
        $logger = new NullLogger();
        $factory = new StrategyFactory($clientWrapper, $logger, 'json');
        self::assertSame($clientWrapper, $factory->getClientWrapper());
        self::assertSame($logger, $factory->getLogger());
        self::assertEquals(
            ['abs', 'local', 's3', 'workspace-abs', 'workspace-redshift',
                'workspace-snowflake', 'workspace-synapse', 'workspace-exasol', 'workspace-teradata'],
            array_keys($factory->getStrategyMap())
        );
    }

    public function testGetFileStrategyFail()
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN),
            ),
            new NullLogger(),
            'json'
        );
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('The project does not support "local" file input backend.');
        $factory->getFileInputStrategy(
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
    }

    public function testGetFileStrategySuccess()
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN),
            ),
            new NullLogger(),
            'json'
        );
        $factory->addProvider(new NullProvider(), [StrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA])]);
        self::assertInstanceOf(
            LocalFile::class,
            $factory->getFileInputStrategy(
                StrategyFactory::LOCAL,
                new InputFileStateList([])
            )
        );
    }

    public function testGetTableStrategyFail()
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN),
            ),
            new NullLogger(),
            'json'
        );
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('The project does not support "local" table input backend.');
        $factory->getTableInputStrategy(StrategyFactory::LOCAL, 'test', new InputTableStateList([]));
    }

    public function testGetTableStrategySuccess()
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN),
            ),
            new NullLogger(),
            'json'
        );
        $factory->addProvider(new NullProvider(), [StrategyFactory::LOCAL => new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA])]);
        self::assertInstanceOf(
            LocalTable::class,
            $factory->getTableInputStrategy(StrategyFactory::LOCAL, 'test', new InputTableStateList([]))
        );
    }

    public function testAddProviderInvalidStaging()
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN),
            ),
            new NullLogger(),
            'json'
        );
        self::expectException(StagingException::class);
        self::expectExceptionMessage(
            'Staging "0" is unknown. Known types are "abs, local, s3, workspace-abs, ' .
            'workspace-redshift, workspace-snowflake, workspace-synapse, workspace-exasol, workspace-teradata'
        );
        $factory->addProvider(new NullProvider(), [new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA])]);
    }

    public function testGetTableStrategyInvalid()
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN),
            ),
            new NullLogger(),
            'json'
        );
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('Input mapping on type "invalid" is not supported. Supported types are "abs, local,');
        $factory->getTableInputStrategy('invalid', 'test', new InputTableStateList([]));
    }
}
