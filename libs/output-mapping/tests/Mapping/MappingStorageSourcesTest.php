<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Mapping\MappingStorageSources;
use Keboola\OutputMapping\Storage\BucketInfo;
use Keboola\OutputMapping\Storage\TableInfo;
use PHPUnit\Framework\TestCase;

class MappingStorageSourcesTest extends TestCase
{
    public function testBasic(): void
    {
        $bucketInfo = new BucketInfo([
            'id' => 'in.c-main',
            'backend' => 'snowflake',
        ]);

        $table = new TableInfo([
            'id' => 'in.c-main.table',
            'isTyped' => true,
            'primaryKey' => ['id'],
            'columns' => [
                'id',
                'name',
            ],
        ]);

        $source = new MappingStorageSources($bucketInfo, $table);
        self::assertEquals($bucketInfo, $source->getBucket());
        self::assertTrue($source->didTableExistBefore());
        self::assertEquals($table, $source->getTable());

        $source = new MappingStorageSources($bucketInfo, null);
        self::assertEquals($bucketInfo, $source->getBucket());
        self::assertFalse($source->didTableExistBefore());
        self::assertNull($source->getTable());
    }
}
