<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Generator;
use Keboola\OutputMapping\DeferredTasks\FailedLoadTableDecider;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class FailedLoadTableDeciderTest extends TestCase
{
    public function decideProvider(): Generator
    {
        yield 'table fresh and empty' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'metadata' => [],
            ],
            'freshlyCreated' => true,
            'expectedResult' => true,
        ];
        yield 'table fresh and null empty' => [
            'tableInfo' => [
                'rowsCount' => null,
                'metadata' => [],
            ],
            'freshlyCreated' => true,
            'expectedResult' => true,
        ];
        yield 'table fresh and not empty' => [
            'tableInfo' => [
                'rowsCount' => 1,
                'metadata' => [],
            ],
            'freshlyCreated' => true,
            'expectedResult' => false,
        ];
        yield 'table fresh and not metadata not empty' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'metadata' => ['a' => 'b'],
            ],
            'freshlyCreated' => true,
            'expectedResult' => false,
        ];
        yield 'table not fresh and empty' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'metadata' => [],
            ],
            'freshlyCreated' => false,
            'expectedResult' => false,
        ];
    }

    /** @dataProvider decideProvider */
    public function testDecide(array $tableInfo, bool $freshlyCreated, bool $expectedResult): void
    {
        $logger = new TestLogger();
        $client = self::createMock(Client::class);
        $client->expects(self::once())->method('getTable')
            ->with('in.c-test.table')
            ->willReturn($tableInfo);
        $task = new LoadTableTask(new MappingDestination('in.c-test.table'), [], $freshlyCreated);
        $result = FailedLoadTableDecider::decideTableDelete($logger, $client, $task);
        self::assertSame($expectedResult, $result);
    }

    public function testDecideNonExistent(): void
    {
        $logger = new TestLogger();
        $client = self::createMock(Client::class);
        $client->expects(self::once())->method('getTable')
            ->with('in.c-test.table')
            ->willThrowException(new ClientException('Table not foundd', 404));
        $task = new LoadTableTask(new MappingDestination('in.c-test.table'), [], true);
        $result = FailedLoadTableDecider::decideTableDelete($logger, $client, $task);
        self::assertSame(false, $result);
    }
}
