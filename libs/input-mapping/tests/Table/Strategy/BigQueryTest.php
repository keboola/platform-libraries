<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\BigQuery;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsStorageBackend;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Psr\Log\NullLogger;

#[NeedsStorageBackend('bigquery')]
class BigQueryTest extends AbstractTestCase
{
    #[NeedsTestTables]
    public function testBigQueryDownloadTableAsView(): void
    {
        $strategy = new BigQuery(
            $this->clientWrapper,
            new NullLogger(),
            $this->createMock(WorkspaceStagingInterface::class),
            $this->createMock(FileStagingInterface::class),
            new InputTableStateList([]),
            'test',
            FileFormat::Json,
        );
        $result = $strategy->downloadTable(new RewrittenInputTableOptions(
            [
                'source' => $this->firstTableId,
                'destination' => 'my-table',
            ],
            $this->firstTableId,
            (int) $this->clientWrapper->getDefaultBranch()->id,
            $this->clientWrapper->getBasicClient()->getTable($this->firstTableId),
        ));

        self::assertEquals(
            [
                'table' => [
                    new RewrittenInputTableOptions(
                        [
                            'source' => $this->firstTableId,
                            'destination' => 'my-table',
                        ],
                        $this->firstTableId,
                        (int) $this->clientWrapper->getDefaultBranch()->id,
                        $this->clientWrapper->getBasicClient()->getTable($this->firstTableId),
                    ),
                    [
                        'overwrite' => false,
                    ],
                ],
                'type' => 'VIEW',
            ],
            $result,
        );
    }

    #[NeedsTestTables]
    public function testBigQueryDownloadTableAliasUsesCopy(): void
    {
        $strategy = new BigQuery(
            $this->clientWrapper,
            new NullLogger(),
            $this->createMock(WorkspaceStagingInterface::class),
            $this->createMock(FileStagingInterface::class),
            new InputTableStateList([]),
            'test',
            FileFormat::Json,
        );

        $aliasId = $this->clientWrapper->getBasicClient()->createAliasTable(
            $this->testBucketId,
            $this->firstTableId,
            'table-alias',
        );

        // Alias tables in current project use COPY instead of VIEW
        $result = $strategy->downloadTable(new RewrittenInputTableOptions(
            [
                'source' => $aliasId,
                'destination' => 'my-table',
            ],
            $aliasId,
            (int) $this->clientWrapper->getDefaultBranch()->id,
            $this->clientWrapper->getBasicClient()->getTable($aliasId),
        ));

        self::assertEquals(
            [
                'table' => [
                    new RewrittenInputTableOptions(
                        [
                            'source' => $aliasId,
                            'destination' => 'my-table',
                        ],
                        $aliasId,
                        (int) $this->clientWrapper->getDefaultBranch()->id,
                        $this->clientWrapper->getBasicClient()->getTable($aliasId),
                    ),
                    [
                        'overwrite' => false,
                    ],
                ],
                'type' => 'COPY',
            ],
            $result,
        );
    }

    public function testGetWorkspaceType(): void
    {
        $strategy = new BigQuery(
            $this->initClient(),
            new NullLogger(),
            $this->createMock(WorkspaceStagingInterface::class),
            $this->createMock(FileStagingInterface::class),
            new InputTableStateList([]),
            'test',
            FileFormat::Json,
        );

        self::assertEquals('bigquery', $strategy->getWorkspaceType());
    }
}
