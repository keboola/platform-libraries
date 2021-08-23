<?php

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Strategy\S3;
use Psr\Log\NullLogger;

class S3StrategyTest extends AbstractStrategyTest
{
    public function testColumns()
    {
        $strategy = new S3(
            $this->clientWrapper,
            new NullLogger(),
            new NullProvider(),
            new NullProvider(),
            new InputTableStateList([]),
            '.'
        );
        $tableOptions = new InputTableOptions(
            [
                'source' => 'in.c-input-mapping-test-strategy.test1',
                'destination' => 'some-table.csv',
                'columns' => ['Id', 'Name']
            ]
        );
        $result = $strategy->downloadTable($tableOptions);
        self::assertArrayHasKey('jobId', $result);
        self::assertArrayHasKey('table', $result);
        $job = $this->clientWrapper->getBasicClient()->getJob($result['jobId']);
        self::assertEquals(
            'tableExport',
            $job['operationName']
        );
        self::assertEquals(
            ['Id', 'Name'],
            $job['operationParams']['export']['columns']
        );
    }

    public function testColumnsExtended()
    {
        $strategy = new S3(
            $this->clientWrapper,
            new NullLogger(),
            new NullProvider(),
            new NullProvider(),
            new InputTableStateList([]),
            '.'
        );
        $tableOptions = new InputTableOptions(
            [
                'source' => 'in.c-input-mapping-test-strategy.test1',
                'destination' => 'some-table.csv',
                'column_types' => [
                    [
                        'source' => 'Id',
                        'destination' => 'myid',
                        'type' => 'VARCHAR'
                    ],
                    [
                        'source' => 'Name',
                        'destination' => 'myname',
                        'type' => 'NUMERIC'
                    ],
                ],
            ]
        );
        $result = $strategy->downloadTable($tableOptions);
        self::assertArrayHasKey('jobId', $result);
        self::assertArrayHasKey('table', $result);
        $job = $this->clientWrapper->getBasicClient()->getJob($result['jobId']);
        self::assertEquals(
            'tableExport',
            $job['operationName']
        );
        self::assertEquals(
            ['Id', 'Name'],
            $job['operationParams']['export']['columns']
        );
    }
}
