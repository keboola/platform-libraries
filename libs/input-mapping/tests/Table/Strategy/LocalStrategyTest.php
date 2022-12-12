<?php

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\InputMapping\Table\Strategy\Local;
use Psr\Log\NullLogger;

class LocalStrategyTest extends AbstractStrategyTest
{
    private function getProvider()
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

    public function testColumns()
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
                'source' => 'in.c-input-mapping-test-strategy.test1',
                'destination' => 'some-table.csv',
                'columns' => ['Id', 'Name']
            ]
        );
        $result = $strategy->downloadTable($tableOptions);
        self::assertEquals(
            [
                'tableId' => 'in.c-input-mapping-test-strategy.test1',
                'destination' => $this->temp->getTmpFolder() . '/boo/some-table.csv',
                'exportOptions' => [
                    'columns' => ['Id', 'Name'],
                    'overwrite' => false,
                    'format' => 'rfc',
                ],
            ],
            $result
        );
    }

    public function testColumnsExtended()
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
        self::assertEquals(
            [
                'tableId' => 'in.c-input-mapping-test-strategy.test1',
                'destination' => $this->temp->getTmpFolder() . '/boo/some-table.csv',
                'exportOptions' => [
                    'columns' => ['Id', 'Name'],
                    'overwrite' => false,
                    'format' => 'rfc',
                ],
            ],
            $result
        );
    }
}
