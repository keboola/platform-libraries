<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks;

use Generator;
use Keboola\OutputMapping\DeferredTasks\FailedLoadTableDecider;
use Keboola\OutputMapping\DeferredTasks\TableWriter\LoadTableTask;
use Keboola\OutputMapping\Writer\Table\MappingDestination;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class FailedLoadTableDeciderTest extends TestCase
{
    public function decideProvider(): Generator
    {
        yield 'table fresh and empty' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'isTyped' => false,
                'metadata' => [],
            ],
            'freshlyCreated' => true,
            'expectedResult' => true,
        ];
        yield 'table fresh and null empty' => [
            'tableInfo' => [
                'rowsCount' => null,
                'isTyped' => false,
                'metadata' => [],
            ],
            'freshlyCreated' => true,
            'expectedResult' => true,
        ];
        yield 'table fresh and not empty' => [
            'tableInfo' => [
                'rowsCount' => 1,
                'isTyped' => false,
                'metadata' => [],
            ],
            'freshlyCreated' => true,
            'expectedResult' => false,
        ];
        yield 'table fresh and not metadata not empty' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'isTyped' => false,
                'metadata' => [
                    [
                        'key' => 'KBC.createdBy.component.id',
                        'value' => 'keboola.ex-db-snowflake',
                        'provider' => 'system',
                    ],
                ],
            ],
            'freshlyCreated' => true,
            'expectedResult' => false,
        ];
        // A description sent in the create-table-definition payload is mirrored by Storage into a
        // KBC.description metadata row under the "storage" provider at create time, before any load.
        yield 'non-typed table fresh and storage KBC.description metadata' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'isTyped' => false,
                'metadata' => [
                    [
                        'key' => 'KBC.description',
                        'value' => 'table description',
                        'provider' => 'storage',
                    ],
                ],
            ],
            'freshlyCreated' => true,
            'expectedResult' => true,
        ];
        yield 'non-typed table fresh and component KBC.description metadata' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'isTyped' => false,
                'metadata' => [
                    [
                        'key' => 'KBC.description',
                        'value' => 'table description',
                        'provider' => 'keboola.ex-db-snowflake',
                    ],
                ],
            ],
            'freshlyCreated' => true,
            'expectedResult' => false,
        ];
        yield 'typed table fresh with storage KBC.dataTypesEnabled and KBC.description metadata' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'isTyped' => true,
                'metadata' => [
                    [
                        'key' => 'KBC.dataTypesEnabled',
                        'value' => true,
                        'provider' => 'storage',
                    ],
                    [
                        'key' => 'KBC.description',
                        'value' => 'table description',
                        'provider' => 'storage',
                    ],
                ],
            ],
            'freshlyCreated' => true,
            'expectedResult' => true,
        ];
        yield 'typed table fresh and storage KBC.dataTypesEnabled metadata' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'isTyped' => true,
                'metadata' => [
                    [
                        'key' => 'KBC.dataTypesEnabled',
                        'value' => true,
                        'provider' => 'storage',
                    ],
                ],
            ],
            'freshlyCreated' => true,
            'expectedResult' => true,
        ];
        yield 'typed table fresh and user KBC.dataTypesEnabled metadata' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'isTyped' => true,
                'metadata' => [
                    [
                        'key' => 'KBC.dataTypesEnabled',
                        'value' => true,
                        'provider' => 'user',
                    ],
                ],
            ],
            'freshlyCreated' => true,
            'expectedResult' => false,
        ];
        yield 'typed table fresh and storage metadata' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'isTyped' => true,
                'metadata' => [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'VARCHAR',
                        'provider' => 'storage',
                    ],
                    [
                        'key' => 'KBC.datatype.type2',
                        'value' => 'VARCHAR2',
                        'provider' => 'storage',
                    ],
                ],
            ],
            'freshlyCreated' => true,
            'expectedResult' => false,
        ];
        yield 'typed table fresh and user metadata' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'isTyped' => true,
                'metadata' => [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'VARCHAR',
                        'provider' => 'extractor',
                    ],
                    [
                        'key' => 'KBC.datatype.type2',
                        'value' => 'VARCHAR2',
                        'provider' => 'storage',
                    ],
                ],
            ],
            'freshlyCreated' => true,
            'expectedResult' => false,
        ];
        yield 'typed table fresh and empty metadata' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'isTyped' => true,
                'metadata' => [],
            ],
            'freshlyCreated' => true,
            'expectedResult' => true,
        ];
        yield 'typed table not fresh and empty metadata' => [
            'tableInfo' => [
                'rowsCount' => 0,
                'isTyped' => true,
                'metadata' => [],
            ],
            'freshlyCreated' => false,
            'expectedResult' => false,
        ];
        yield 'typed table fresh, storage metadata and not empty' => [
            'tableInfo' => [
                'rowsCount' => 1,
                'isTyped' => true,
                'metadata' => [
                    [
                        'key' => 'KBC.datatype.type',
                        'value' => 'VARCHAR',
                        'provider' => 'storage',
                    ],
                    [
                        'key' => 'KBC.datatype.type2',
                        'value' => 'VARCHAR2',
                        'provider' => 'storage',
                    ],
                ],
            ],
            'freshlyCreated' => true,
            'expectedResult' => false,
        ];
    }

    /** @dataProvider decideProvider */
    public function testDecide(array $tableInfo, bool $freshlyCreated, bool $expectedResult): void
    {
        $client = self::createMock(Client::class);
        $client->expects(self::once())->method('getTable')
            ->with('in.c-test.table')
            ->willReturn($tableInfo);
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($client);
        $task = new LoadTableTask(new MappingDestination('in.c-test.table'), [], $freshlyCreated);
        $result = FailedLoadTableDecider::decideTableDelete(new Logger('testLogger'), $clientWrapperMock, $task);
        self::assertSame($expectedResult, $result);
    }

    public function testDecideNonExistent(): void
    {
        $client = self::createMock(Client::class);
        $client->expects(self::once())->method('getTable')
            ->with('in.c-test.table')
            ->willThrowException(new ClientException('Table not foundd', 404));
        $clientWrapperMock = self::createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')
            ->willReturn($client);
        $task = new LoadTableTask(new MappingDestination('in.c-test.table'), [], true);
        $result = FailedLoadTableDecider::decideTableDelete(new Logger('testLogger'), $clientWrapperMock, $task);
        self::assertSame(false, $result);
    }
}
