<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\InputMapping\Tests\Needs\NeedsStorageBackend;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StorageApi\ClientException;

#[NeedsStorageBackend('bigquery')]
class DownloadTablesWorkspaceBigQueryTest extends AbstractTestCase
{
    #[NeedsTestTables, NeedsEmptyOutputBucket]
    public function testTablesBigQueryBackend(): void
    {
        $reader = new Reader(
            $this->getWorkspaceStagingFactory(
                null,
                'json',
                $this->testLogger,
                [AbstractStrategyFactory::WORKSPACE_BIGQUERY, 'bigquery'],
            ),
        );
        $configuration = new InputTableOptionsList([
            [
                'source' => $this->firstTableId,
                'destination' => 'test1',
            ],
        ]);

        $result = $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::WORKSPACE_BIGQUERY,
            new ReaderOptions(true),
        );

        $metrics = $result->getMetrics()?->getTableMetrics();
        self::assertNotNull($metrics);
        self::assertCount(1, iterator_to_array($metrics));

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals($this->firstTableId, $manifest['id']);
        /* we want to check that the table exists in the workspace, so we try to load it, which fails, because of
            the _timestamp columns, but that's okay. It means that the table is indeed in the workspace. */
        try {
            $this->clientWrapper->getTableAndFileStorageClient()->createTableAsyncDirect(
                $this->emptyOutputBucketId,
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1'],
            );
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('Invalid columns: _timestamp:', $e->getMessage());
        }

        self::assertTrue($this->testHandler->hasInfoThatContains('Using "workspace-bigquery" table input staging.'));
        self::assertTrue($this->testHandler->hasInfoThatContains(sprintf(
            'Table "%s" will be created as view.',
            $this->firstTableId,
        )));
        self::assertTrue($this->testHandler->hasInfoThatContains('Copying 1 tables to workspace.'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Processed 1 workspace exports.'));
        // test that the clone jobs are merged into a single one
        sleep(2);
        $jobs = $this->clientWrapper->getTableAndFileStorageClient()->listJobs(['limit' => 20]);
        $params = null;
        foreach ($jobs as $job) {
            if ($job['operationName'] === 'workspaceLoad') {
                $params = $job['operationParams'];
                break;
            }
        }
        self::assertNotEmpty($params);
        self::assertEquals(1, count($params['input']));
        self::assertEquals('test1', $params['input'][0]['destination']);
    }
}
