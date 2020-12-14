<?php

namespace Keboola\InputMapping\Tests\Staging;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Exception\StagingException;
use Keboola\InputMapping\File\Strategy\Local as LocalFile;
use Keboola\InputMapping\Table\Strategy\Local as LocalTable;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class StrategyFactoryTest extends TestCase
{
    public function testAccessors()
    {
        $clientWrapper = new ClientWrapper(
            new Client(["token" => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]),
            null,
            new NullLogger(),
            ''
        );
        $logger = new NullLogger();
        $factory = new StrategyFactory($clientWrapper, $logger, 'json');
        self::assertSame($clientWrapper, $factory->getClientWrapper());
        self::assertSame($logger, $factory->getLogger());
        self::assertEquals(
            ['abs', 'local', 's3', 'workspace-abs', 'workspace-redshift', 'workspace-snowflake', 'workspace-synapse'],
            array_keys($factory->getStrategyMap())
        );
    }

    public function testGetFileStrategyFail()
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new Client(["token" => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('The project does not support "local" file backend.');
        $factory->getFileStrategy(StrategyFactory::LOCAL);
    }

    public function testGetFileStrategySuccess()
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new Client(["token" => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );
        $factory->addProvider(new NullProvider(), [StrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA])]);
        self::assertInstanceOf(
            LocalFile::class,
            $factory->getFileStrategy(StrategyFactory::LOCAL)
        );
    }

    public function testGetTableStrategyFail()
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new Client(["token" => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('The project does not support "local" table backend.');
        $factory->getTableStrategy(StrategyFactory::LOCAL, 'test', new InputTableStateList([]));
    }

    public function testGetTableStrategySuccess()
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new Client(["token" => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );
        $factory->addProvider(new NullProvider(), [StrategyFactory::LOCAL => new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA])]);
        self::assertInstanceOf(
            LocalTable::class,
            $factory->getTableStrategy(StrategyFactory::LOCAL, 'test', new InputTableStateList([]))
        );
    }

    public function testAddProviderInvalidStaging()
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new Client(["token" => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );
        self::expectException(StagingException::class);
        self::expectExceptionMessage('Staging "0" is unknown. Known types are "abs, local, s3, workspace-abs,');
        $factory->addProvider(new NullProvider(), [new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA])]);
    }

    public function testGetTableStrategyInvalid()
    {
        $factory = new StrategyFactory(
            new ClientWrapper(
                new Client(["token" => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );
        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('Input mapping on type "invalid" is not supported. Supported types are "abs, local,');
        $factory->getTableStrategy('invalid', 'test', new InputTableStateList([]));
    }
}
