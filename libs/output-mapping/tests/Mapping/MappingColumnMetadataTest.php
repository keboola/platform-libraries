<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Mapping\MappingColumnMetadata;
use PHPUnit\Framework\TestCase;

class MappingColumnMetadataTest extends TestCase
{
    public function testGetters(): void
    {
        $columnMetadata = new MappingColumnMetadata(
            'columnName',
            [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.description',
                    'value' => 'Some description of the columnName.',
                ],
            ],
        );

        self::assertSame('columnName', $columnMetadata->getColumnName());
        self::assertSame(
            [
                [
                    'key' => 'KBC.datatype.type',
                    'value' => 'STRING',
                ],
                [
                    'key' => 'KBC.description',
                    'value' => 'Some description of the columnName.',
                ],
            ],
            $columnMetadata->getMetadata(),
        );
    }
}
