<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\Local;
use Keboola\InputMapping\Table\Strategy\TableExportQueue;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Psr\Log\NullLogger;

class LocalStrategyTest extends AbstractTestCase
{
    private function getProvider(): FileStagingInterface
    {
        $mockLocal = $this->createMock(FileStagingInterface::class);
        $mockLocal->method('getPath')->willReturnCallback(
            fn() => $this->temp->getTmpFolder(),
        );

        return $mockLocal;
    }

    #[NeedsTestTables]
    public function testColumns(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new NullLogger(),
            $this->getProvider(),
            $this->getProvider(),
            new InputTableStateList([]),
            'boo',
            FileFormat::Json,
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
        $queue = $strategy->prepareAndExecuteTableLoads([$tableOptions], true);
        self::assertInstanceOf(TableExportQueue::class, $queue);
        $jobIds = $queue->getJobIds();
        self::assertCount(1, $jobIds);
        self::assertEquals(
            [
                'tableId' => $this->firstTableId,
                'destination' => $this->temp->getTmpFolder() . '/boo/some-table.csv',
                'exportOptions' => [
                    'columns' => ['Id', 'Name'],
                    'overwrite' => false,
                    'sourceBranchId' => (int) $this->clientWrapper->getDefaultBranch()->id,
                ],
            ],
            $queue->exportJobs[$jobIds[0]],
        );
    }

    #[NeedsTestTables]
    public function testColumnsExtended(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new NullLogger(),
            $this->getProvider(),
            $this->getProvider(),
            new InputTableStateList([]),
            'boo',
            FileFormat::Json,
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
        $queue = $strategy->prepareAndExecuteTableLoads([$tableOptions], true);
        self::assertInstanceOf(TableExportQueue::class, $queue);
        $jobIds = $queue->getJobIds();
        self::assertCount(1, $jobIds);
        self::assertEquals(
            [
                'tableId' => $this->firstTableId,
                'destination' => $this->temp->getTmpFolder() . '/boo/some-table.csv',
                'exportOptions' => [
                    'columns' => ['Id', 'Name'],
                    'overwrite' => false,
                    'sourceBranchId' => (int) $this->clientWrapper->getDefaultBranch()->id,
                ],
            ],
            $queue->exportJobs[$jobIds[0]],
        );
    }
}
