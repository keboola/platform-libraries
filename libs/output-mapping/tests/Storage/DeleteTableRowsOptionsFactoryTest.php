<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Storage\DeleteTableRowsOptionsFactory;
use PHPUnit\Framework\TestCase;

class DeleteTableRowsOptionsFactoryTest extends TestCase
{
    public function testCreateFromLegacyDeleteWhereColumn(): void
    {
        self::assertSame(
            [
                'whereColumn' => 'test_column',
                'whereOperator' => 'eq',
                'whereValues' => ['value1', 'value2'],
            ],
            DeleteTableRowsOptionsFactory::createFromLegacyDeleteWhereColumn(
                'test_column',
                'eq',
                ['value1', 'value2'],
            ),
        );
    }
}
