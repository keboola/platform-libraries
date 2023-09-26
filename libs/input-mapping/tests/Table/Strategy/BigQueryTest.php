<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\BigQuery;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsStorageBackend;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
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
            new NullProvider(),
            new NullProvider(),
            new InputTableStateList([]),
            'test',
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
                'type' => 'view',
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
            new NullProvider(),
            new NullProvider(),
            new InputTableStateList([]),
            'test',
        );

        $aliasId = $this->clientWrapper->getBasicClient()->createAliasTable(
            $this->testBucketId,
            $this->firstTableId,
            'table-alias',
        );

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
                'type' => 'copy',
            ],
            $result,
        );
    }
}
