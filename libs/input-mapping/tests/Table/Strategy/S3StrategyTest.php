<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\S3;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Psr\Log\NullLogger;

class S3StrategyTest extends AbstractTestCase
{
    #[NeedsTestTables]
    public function testColumns(): void
    {
        $strategy = new S3(
            $this->clientWrapper,
            new NullLogger(),
            new NullProvider(),
            new NullProvider(),
            new InputTableStateList([]),
            '.',
        );
        $tableOptions = new RewrittenInputTableOptions(
            [
                'source' => $this->firstTableId,
                'destination' => 'some-table.csv',
                'columns' => ['Id', 'Name'],
            ],
            $this->firstTableId,
            (int) $this->clientWrapper->getDefaultBranch()->id,
            $this->clientWrapper->getBasicClient()->getTable($this->firstTableId),
        );
        $result = $strategy->downloadTable($tableOptions);
        self::assertArrayHasKey('jobId', $result);
        self::assertArrayHasKey('table', $result);
        /** @var array{
         *     operationName: string,
         *     operationParams: array{
         *         export: array{
         *             columns: string[]
         *         }
         *     }
         * } $job
         */
        $job = $this->clientWrapper->getBranchClient()->getJob($result['jobId']);
        self::assertEquals(
            'tableExport',
            $job['operationName'],
        );
        self::assertEquals(
            ['Id', 'Name'],
            $job['operationParams']['export']['columns'],
        );
    }

    #[NeedsTestTables]
    public function testColumnsExtended(): void
    {
        $strategy = new S3(
            $this->clientWrapper,
            new NullLogger(),
            new NullProvider(),
            new NullProvider(),
            new InputTableStateList([]),
            '.',
        );
        $tableOptions = new RewrittenInputTableOptions(
            [
                'source' => $this->firstTableId,
                'destination' => 'some-table.csv',
                'column_types' => [
                    [
                        'source' => 'Id',
                        'destination' => 'myid',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'source' => 'Name',
                        'destination' => 'myname',
                        'type' => 'NUMERIC',
                    ],
                ],
            ],
            $this->firstTableId,
            (int) $this->clientWrapper->getDefaultBranch()->id,
            $this->clientWrapper->getBasicClient()->getTable($this->firstTableId),
        );
        $result = $strategy->downloadTable($tableOptions);
        self::assertArrayHasKey('jobId', $result);
        self::assertArrayHasKey('table', $result);
        /** @var array{
         *     operationName: string,
         *     operationParams: array{
         *         export: array{
         *             columns: string[]
         *         }
         *     }
         * } $job
         */
        $job = $this->clientWrapper->getBranchClient()->getJob($result['jobId']);
        self::assertEquals(
            'tableExport',
            $job['operationName'],
        );
        self::assertEquals(
            ['Id', 'Name'],
            $job['operationParams']['export']['columns'],
        );
    }
}
