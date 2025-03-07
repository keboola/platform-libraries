<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\DeferredTasks\Metadata;

use Generator;
use Keboola\OutputMapping\DeferredTasks\Metadata\SchemaColumnsMetadata;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use PHPUnit\Framework\TestCase;

class SchemaColumnsMetadataTest extends TestCase
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
        $matcher = self::exactly($expectedApiCalls);
        $metadataClientMock->expects($matcher)
            ->method('postTableMetadataWithColumns')
            ->willReturnCallback(
                function (TableMetadataUpdateOptions $options) use ($matcher, $expectedColumnsMetadata): array {
                    self::assertSame('in.c-testApply.table', $options->getTableId());
                    self::assertSame(
                        [
                            'provider' => 'keboola.sample-component',
                            'columnsMetadata' => $expectedColumnsMetadata[$matcher->getInvocationCount() - 1],
                        ],
                        $options->toParamsArray(),
                    );
                    return $expectedColumnsMetadata;
                },
            )
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

        $columnsMetadata = new SchemaColumnsMetadata(
            self::TEST_TABLE_ID,
            self::TEST_PROVIDER,
            $schemaMetadata,
        );

        $columnsMetadata->apply($metadataClientMock, $bulkSize);
    }
}
