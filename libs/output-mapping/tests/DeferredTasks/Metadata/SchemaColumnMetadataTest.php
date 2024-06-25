<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks\Metadata;

use Generator;
use Keboola\OutputMapping\DeferredTasks\Metadata\ColumnMetadata;
use Keboola\OutputMapping\DeferredTasks\Metadata\SchemaColumnMetadata;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use PHPUnit\Framework\TestCase;

class SchemaColumnMetadataTest extends TestCase
{
    private const TEST_TABLE_ID = 'in.c-testApply.table';
    private const TEST_PROVIDER = 'keboola.sample-component';

    public function applyDataProvider(): Generator
    {
        yield 'load at once' => [
            'bulkSize' => 10,
            'expectedApiCalls' => 1,
            'expectedColumnsMetadata' => [
                [
                    'col1' => [
                        [
                            'columnName' => 'col1',
                            'key' => 'key1',
                            'value' => 'val1',
                        ],
                        [
                            'columnName' => 'col1',
                            'key' => 'key2',
                            'value' => 'val2',
                        ],
                    ],
                    'col2' => [
                        [
                            'columnName' => 'col2',
                            'key' => 'KBC.description',
                            'value' => 'col2 description',
                        ],
                    ],
                ],
            ],
        ];

        yield 'load in chunks' => [
            'bulkSize' => 1,
            'expectedApiCalls' => 2,
            'expectedColumnsMetadata' => [
                [
                    'col1' => [
                        [
                            'columnName' => 'col1',
                            'key' => 'key1',
                            'value' => 'val1',
                        ],
                        [
                            'columnName' => 'col1',
                            'key' => 'key2',
                            'value' => 'val2',
                        ],
                    ],
                ],
                [
                    'col2' => [
                        [
                            'columnName' => 'col2',
                            'key' => 'KBC.description',
                            'value' => 'col2 description',
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

        $schemaMetadata = [
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col1',
                'metadata' => [
                    'key1' => 'val1',
                    'key2' => 'val2',
                ],
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col2',
                'description' => 'col2 description',
            ]),
            new MappingFromConfigurationSchemaColumn([
                'name' => 'col3',
            ]),
        ];

        $columnMetadata = new SchemaColumnMetadata(
            self::TEST_TABLE_ID,
            self::TEST_PROVIDER,
            $schemaMetadata,
        );

        $columnMetadata->apply($metadataClientMock, $bulkSize);
    }
}
