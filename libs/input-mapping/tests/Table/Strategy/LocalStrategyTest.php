<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Strategy\Local;
use Keboola\InputMapping\Tests\AbstractTestCase;
use Keboola\InputMapping\Tests\Needs\NeedsTestTables;
use Psr\Log\NullLogger;

class LocalStrategyTest extends AbstractTestCase
{
    private function getProvider(): ProviderInterface
    {
        $mockLocal = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getPath'])
            ->getMock();
        $mockLocal->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            }
        );
        /** @var ProviderInterface $mockLocal */
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
            'boo'
        );
        $tableOptions = new InputTableOptions(
            [
                'source' => $this->firstTableId,
                'destination' => 'some-table.csv',
                'columns' => ['Id', 'Name'],
            ]
        );
        $result = $strategy->downloadTable($tableOptions);
        self::assertEquals(
            [
                'tableId' => $this->firstTableId,
                'destination' => $this->temp->getTmpFolder() . '/boo/some-table.csv',
                'exportOptions' => [
                    'columns' => ['Id', 'Name'],
                    'overwrite' => false,
                ],
            ],
            $result
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
            'boo'
        );
        $tableOptions = new InputTableOptions(
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
            ]
        );
        $result = $strategy->downloadTable($tableOptions);
        self::assertEquals(
            [
                'tableId' => $this->firstTableId,
                'destination' => $this->temp->getTmpFolder() . '/boo/some-table.csv',
                'exportOptions' => [
                    'columns' => ['Id', 'Name'],
                    'overwrite' => false,
                ],
            ],
            $result
        );
    }
}
