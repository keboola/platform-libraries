<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks\Metadata;

use Generator;
use Keboola\OutputMapping\DeferredTasks\Metadata\ColumnsMetadata;
use Keboola\OutputMapping\Mapping\MappingColumnMetadata;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use PHPUnit\Framework\TestCase;

class ColumnsMetadataTest extends TestCase
{
    public function applyDataProvider(): Generator
    {
        yield 'load at once' => [
            'bulkSize' => 10,
            'expectedApiCalls' => 1,
            'expectedColumnsMetadata' => [
                [
                    'id' => [
                        [
                            'columnName' => 'id',
                            'key' => 'timestamp',
                            'value' => '1674226231',
                        ],
                    ],
                    '0' => [
                        [
                            'columnName' => '0',
                            'key' => 'timestamp',
                            'value' => '1674226231',
                        ],
                    ],
                    'aa_caa' => [
                        [
                            'columnName' => 'aa_caa',
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ],
                        [
                            'columnName' => 'aa_caa',
                            'key' => '1',
                            'value' => '',
                        ],
                    ],
                ],
            ],
        ];

        yield 'load in chunks' => [
            'bulkSize' => 1,
            'expectedApiCalls' => 3,
            'expectedColumnsMetadata' => [
                [
                    'id' => [
                        [
                            'columnName' => 'id',
                            'key' => 'timestamp',
                            'value' => '1674226231',
                        ],
                    ],
                ],
                [
                    '0' => [
                        [
                            'columnName' => '0',
                            'key' => 'timestamp',
                            'value' => '1674226231',
                        ],
                    ],
                ],
                [
                    'aa_caa' => [
                        [
                            'columnName' => 'aa_caa',
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ],
                        [
                            'columnName' => 'aa_caa',
                            'key' => '1',
                            'value' => '',
                        ],
                    ],
                ],
            ],
        ];
    }

    /**
     * @dataProvider applyDataProvider
     */
    public function testApply(int $bulkSize, int $expectedApiCalls, array $expectedColumnsMetadata): void
    {
        $metadataClientMock = $this->createMock(Metadata::class);
        $metadataClientMock->expects(self::exactly($expectedApiCalls))
            ->method('postTableMetadataWithColumns')
            ->withConsecutive(...array_map(function (array $columnsMetadata): array {
                return [self::callback(function (TableMetadataUpdateOptions $options) use ($columnsMetadata) {
                    self::assertSame('in.c-testApply.table', $options->getTableId());
                    self::assertSame(
                        [
                            'provider' => 'keboola.sample-component',
                            'columnsMetadata' => $columnsMetadata,
                        ],
                        $options->toParamsArray(),
                    );
                    return true;
                })];
            }, $expectedColumnsMetadata))
        ;

        $columnMetadata = new ColumnsMetadata(
            'in.c-testApply.table',
            'keboola.sample-component',
            [
                new MappingColumnMetadata(
                    'id',
                    [
                        [
                            'key' => 'timestamp',
                            'value' => '1674226231',
                        ],
                    ],
                ),
                new MappingColumnMetadata(
                    '0',
                    [
                        [
                            'key' => 'timestamp',
                            'value' => '1674226231',
                        ],
                    ],
                ),
                new MappingColumnMetadata(
                    'aa_caa',
                    [
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ],
                        [
                            'key' => '1',
                            'value' => '',
                        ],
                    ],
                ),
            ],
        );

        $columnMetadata->apply($metadataClientMock, $bulkSize);
    }
}
