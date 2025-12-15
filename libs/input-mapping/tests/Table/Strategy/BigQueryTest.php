<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Exception\InvalidInputException;
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
use RuntimeException;

#[NeedsStorageBackend('bigquery')]
class BigQueryTest extends AbstractTestCase
{
    #[NeedsTestTables]
    public function testBigQueryDownloadTableAsViewWithFeatureFlag(): void
    {
        if (!$this->clientWrapper->getToken()->hasFeature('bigquery-default-im-view')) {
            throw new RuntimeException('Project does not have bigquery-default-im-view feature.');
        }

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
    public function testBigQueryDownloadTableAlias(): void
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

        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(sprintf(
            'Table "%s" is an alias, which is not supported when loading Bigquery tables.',
            $aliasId,
        ));
        $strategy->downloadTable(new RewrittenInputTableOptions(
            [
                'source' => $aliasId,
                'destination' => 'my-table',
            ],
            $aliasId,
            (int) $this->clientWrapper->getDefaultBranch()->id,
            $this->clientWrapper->getBasicClient()->getTable($aliasId),
        ));
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
