<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Generator;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhereFilterFromStorage;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhereFilterFromWorkspace;
use PHPUnit\Framework\TestCase;

class MappingFromConfigurationDeleteWhereFilterFromStorageTest extends TestCase
{
    public static function configurationProvider(): Generator
    {
        yield 'minimal configuration' => [
            [
                'column' => 'columnName',
                'operator' => 'ne',
                'values_from_storage' => [
                    'bucket_id' => 'in.c-main',
                    'table' => 'storageTable',
                ],
            ],
            [
                'column' => 'columnName',
                'operator' => 'ne',
                'bucket_id' => 'in.c-main',
                'storage_table' => 'storageTable',
                'storage_column' => null,
            ],
        ];

        yield 'full configuration' => [
            [
                'column' => 'columnName',
                'operator' => 'ne',
                'values_from_storage' => [
                    'bucket_id' => 'in.c-main',
                    'table' => 'storageTable',
                    'column' => 'storaeColumn',
                ],
            ],
            [
                'column' => 'columnName',
                'operator' => 'ne',
                'bucket_id' => 'in.c-main',
                'storage_table' => 'storageTable',
                'storage_column' => 'storaeColumn',
            ],
        ];
    }

    /**
     * @dataProvider configurationProvider
     */
    public function testGetters(array $config, array $expected): void
    {
        $whereFilterFromSet = new MappingFromConfigurationDeleteWhereFilterFromStorage($config);

        self::assertSame($expected['column'], $whereFilterFromSet->getColumn());
        self::assertSame($expected['operator'], $whereFilterFromSet->getOperator());
        self::assertSame($expected['bucket_id'], $whereFilterFromSet->getStorageBucketId());
        self::assertSame($expected['storage_table'], $whereFilterFromSet->getStorageTable());
        self::assertSame($expected['storage_column'], $whereFilterFromSet->getStorageColumn());
    }
}
