<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Psr\Log\Test\TestLogger;

class DownloadTablesWorkspaceRedshiftTest extends DownloadTablesWorkspaceTestAbstract
{
    public function testTablesRedshiftBackend(): void
    {
        $logger = new TestLogger();
        $reader = new Reader(
            $this->getStagingFactory(
                null,
                'json',
                $logger,
                [AbstractStrategyFactory::WORKSPACE_REDSHIFT, 'redshift']
            )
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test1',
                'destination' => 'test1',
                'changed_since' => '-2 days',
                'columns' => ['Id'],
            ],
            [
                'source' => 'in.c-input-mapping-test.test2',
                'destination' => 'test2',
                'column_types' => [
                    [
                        'source' => 'Id',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'source' => 'Name',
                        'type' => 'VARCHAR',
                    ],
                ],
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_REDSHIFT,
            new ReaderOptions(true)
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        /* because of https://keboola.atlassian.net/browse/KBC-228 we have to create redshift bucket to
            unload data from redshift workspace */
        $this->clientWrapper->getBasicClient()->dropBucket('out.c-input-mapping-test');
        $this->clientWrapper->getBasicClient()->createBucket(
            'input-mapping-test',
            Client::STAGE_OUT,
            'Docker Testsuite',
            'redshift'
        );

        self::assertEquals('in.c-input-mapping-test.test1', $manifest['id']);
        // test that the table exists in the workspace
        $tableId = $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1']
        );
        self::assertEquals('out.c-input-mapping-test.test1', $tableId);
        $table = $this->clientWrapper->getBasicClient()->getTable($tableId);
        self::assertEquals(['id'], $table['columns']);

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);
        $tableId = $this->clientWrapper->getBasicClient()->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );
        self::assertEquals('out.c-input-mapping-test.test2', $tableId);

        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test1" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Processed 1 workspace exports.'));
    }

    public function testUseViewFails(): void
    {
        $logger = new TestLogger();
        $reader = new Reader(
            $this->getStagingFactory(
                null,
                'json',
                $logger,
                [AbstractStrategyFactory::WORKSPACE_REDSHIFT, 'redshift']
            )
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test1',
                'destination' => 'test1',
                'use_view' => true,
            ],
        ]);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage(
            'View load for table "test1" using backend "redshift" can\'t be used, only Synapse is supported.'
        );

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_REDSHIFT,
            new ReaderOptions(true)
        );
    }
}
