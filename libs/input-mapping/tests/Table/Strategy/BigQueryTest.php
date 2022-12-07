<?php

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Strategy\BigQuery;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Psr\Log\NullLogger;

class BigQueryTest extends AbstractStrategyTest
{
    public function testBigQueryDownloadTable()
    {
        $clientWrapper = new ClientWrapper(
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN),
        );
        $strategy = new BigQuery(
            $clientWrapper,
            new NullLogger(),
            new NullProvider(),
            new NullProvider(),
            new InputTableStateList([]),
            'test'
        );
        $result = $strategy->downloadTable(new InputTableOptions(
            [
                'source' => 'in.c-input-mapping-test-strategy.test1',
                'destination' => 'my-table',
                'columns' => ['foo', 'bar'],
            ]
        ));
        self::assertEquals(
            [
                'table' => [
                    new InputTableOptions(
                        [
                            'source' => 'in.c-input-mapping-test-strategy.test1',
                            'destination' => 'my-table',
                            'columns' => ['foo', 'bar'],
                        ]
                    ),
                    [
                        'columns' => [
                            ['source' => 'foo'],
                            ['source' => 'bar']
                        ],
                        'overwrite' => false,
                    ],
                ],
                'type' => 'copy',
            ],
            $result
        );
    }
}
