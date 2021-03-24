<?php

namespace Keboola\WorkspaceProvider\Tests;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\Strategy\ABSWorkspace as InputFileABSWorkspace;
use Keboola\InputMapping\File\Strategy\Local as InputFileLocal;
use Keboola\InputMapping\Staging\StrategyFactory as InputStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\ABS as InputAbs;
use Keboola\InputMapping\Table\Strategy\ABSWorkspace as InputTableABSWorkspace;
use Keboola\InputMapping\Table\Strategy\Local as InputTableLocal;
use Keboola\InputMapping\Table\Strategy\Redshift as InputTableRedshift;
use Keboola\InputMapping\Table\Strategy\S3 as InputS3;
use Keboola\InputMapping\Table\Strategy\Snowflake as InputTableSnowflake;
use Keboola\InputMapping\Table\Strategy\Synapse as InputTableSynapse;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory as OutputStrategyFactory;
use Keboola\OutputMapping\Writer\File\Strategy\Local as OutputFileLocal;
use Keboola\OutputMapping\Writer\Table\Strategy\AllEncompassingTableStrategy;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\WorkspaceProvider\ProviderInitializer;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class ProviderInitializerTest extends TestCase
{
    public function testInitializeInputLocal()
    {
        $stagingFactory = new InputStrategyFactory(
            new ClientWrapper(
                new Client(['token' => 'foo', 'url' => 'bar']),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );

        $init = new ProviderInitializer();
        $init->initializeInputProviders(
            $stagingFactory,
            InputStrategyFactory::LOCAL,
            'my-test-component',
            'my-test-config',
            [],
            '/tmp/random/data'
        );
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::ABS));
        self::assertInstanceOf(InputAbs::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::ABS, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::S3));
        self::assertInstanceOf(InputS3::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::S3, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-redshift" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_REDSHIFT, '', new InputTableStateList([]));
    }

    public function testInitializeInputRedshift()
    {
        $stagingFactory = new InputStrategyFactory(
            new ClientWrapper(
                new Client(['token' => 'foo', 'url' => 'bar']),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );

        $init = new ProviderInitializer();
        $init->initializeInputProviders(
            $stagingFactory,
            InputStrategyFactory::WORKSPACE_REDSHIFT,
            'my-test-component',
            'my-test-config',
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'aws',
                ],
            ],
            '/tmp/random/data'
        );
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::WORKSPACE_REDSHIFT));
        self::assertInstanceOf(InputTableRedshift::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_REDSHIFT, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE, '', new InputTableStateList([]));
    }

    public function testInitializeInputSnowflake()
    {
        $stagingFactory = new InputStrategyFactory(
            new ClientWrapper(
                new Client(['token' => 'foo', 'url' => 'bar']),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );

        $init = new ProviderInitializer();
        $init->initializeInputProviders(
            $stagingFactory,
            InputStrategyFactory::WORKSPACE_SNOWFLAKE,
            'my-test-component',
            'my-test-config',
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ],
            '/tmp/random/data'
        );
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE));
        self::assertInstanceOf(InputTableSnowflake::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-redshift" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_REDSHIFT, '', new InputTableStateList([]));
    }

    public function testInitializeInputSynapse()
    {
        $stagingFactory = new InputStrategyFactory(
            new ClientWrapper(
                new Client(['token' => 'foo', 'url' => 'bar']),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );

        $init = new ProviderInitializer();
        $init->initializeInputProviders(
            $stagingFactory,
            InputStrategyFactory::WORKSPACE_SYNAPSE,
            'my-test-component',
            'my-test-config',
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ],
            '/tmp/random/data'
        );
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::WORKSPACE_SYNAPSE));
        self::assertInstanceOf(InputTableSynapse::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SYNAPSE, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE, '', new InputTableStateList([]));
    }

    public function testInitializeInputAbs()
    {
        $stagingFactory = new InputStrategyFactory(
            new ClientWrapper(
                new Client(['token' => 'foo', 'url' => 'bar']),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );

        $init = new ProviderInitializer();
        $init->initializeInputProviders(
            $stagingFactory,
            InputStrategyFactory::WORKSPACE_ABS,
            'my-test-component',
            'my-test-config',
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ],
            '/tmp/random/data'
        );

        self::assertInstanceOf(InputFileLocal::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::LOCAL));
        self::assertInstanceOf(InputTableLocal::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::LOCAL, '', new InputTableStateList([])));
        self::assertInstanceOf(InputFileABSWorkspace::class, $stagingFactory->getFileInputStrategy(InputStrategyFactory::WORKSPACE_ABS));
        self::assertInstanceOf(InputTableABSWorkspace::class, $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_ABS, '', new InputTableStateList([])));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table input backend.');
        $this->expectException(InvalidInputException::class);
        $stagingFactory->getTableInputStrategy(InputStrategyFactory::WORKSPACE_SNOWFLAKE, '', new InputTableStateList([]));
    }

    public function testInitializeOutputLocal()
    {
        $stagingFactory = new OutputStrategyFactory(
            new ClientWrapper(
                new Client(['token' => 'foo', 'url' => 'bar']),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );

        $init = new ProviderInitializer();
        $init->initializeOutputProviders(
            $stagingFactory,
            OutputStrategyFactory::LOCAL,
            'my-test-component',
            'my-test-config',
            [],
            '/tmp/random/data'
        );
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(AllEncompassingTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::LOCAL));

        $this->expectExceptionMessage('The project does not support "workspace-redshift" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_REDSHIFT);
    }

    public function testInitializeOutputRedshift()
    {
        $stagingFactory = new OutputStrategyFactory(
            new ClientWrapper(
                new Client(['token' => 'foo', 'url' => 'bar']),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );

        $init = new ProviderInitializer();
        $init->initializeOutputProviders(
            $stagingFactory,
            OutputStrategyFactory::WORKSPACE_REDSHIFT,
            'my-test-component',
            'my-test-config',
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ],
            '/tmp/random/data'
        );
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(AllEncompassingTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::WORKSPACE_REDSHIFT));
        self::assertInstanceOf(AllEncompassingTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_REDSHIFT));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_SNOWFLAKE);
    }

    public function testInitializeOutputSnowflake()
    {
        $stagingFactory = new OutputStrategyFactory(
            new ClientWrapper(
                new Client(['token' => 'foo', 'url' => 'bar']),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );

        $init = new ProviderInitializer();
        $init->initializeOutputProviders(
            $stagingFactory,
            OutputStrategyFactory::WORKSPACE_SNOWFLAKE,
            'my-test-component',
            'my-test-config',
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ],
            '/tmp/random/data'
        );
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(AllEncompassingTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::WORKSPACE_SNOWFLAKE));
        self::assertInstanceOf(AllEncompassingTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_SNOWFLAKE));

        $this->expectExceptionMessage('The project does not support "workspace-redshift" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_REDSHIFT);
    }

    public function testInitializeOutputSynapse()
    {
        $stagingFactory = new OutputStrategyFactory(
            new ClientWrapper(
                new Client(['token' => 'foo', 'url' => 'bar']),
                null,
                new NullLogger(),
                ''
            ),
            new NullLogger(),
            'json'
        );

        $init = new ProviderInitializer();
        $init->initializeOutputProviders(
            $stagingFactory,
            OutputStrategyFactory::WORKSPACE_SYNAPSE,
            'my-test-component',
            'my-test-config',
            [
                'owner' => [
                    'hasSynapse' => true,
                    'hasRedshift' => true,
                    'hasSnowflake' => true,
                    'fileStorageProvider' => 'azure',
                ],
            ],
            '/tmp/random/data'
        );
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(AllEncompassingTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::LOCAL));
        self::assertInstanceOf(OutputFileLocal::class, $stagingFactory->getFileOutputStrategy(OutputStrategyFactory::WORKSPACE_SYNAPSE));
        self::assertInstanceOf(AllEncompassingTableStrategy::class, $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_SYNAPSE));

        $this->expectExceptionMessage('The project does not support "workspace-snowflake" table output backend.');
        $this->expectException(InvalidOutputException::class);
        $stagingFactory->getTableOutputStrategy(OutputStrategyFactory::WORKSPACE_SNOWFLAKE);
    }
}
