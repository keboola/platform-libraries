<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Mapping;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationDeleteWhereFilterFromSet;
use PHPUnit\Framework\TestCase;

class MappingFromConfigurationDeleteWhereFilterFromSetTest extends TestCase
{
    public function testGetters(): void
    {
        $whereFilterFromSet = new MappingFromConfigurationDeleteWhereFilterFromSet(
            [
                'column' => 'columnName',
                'operator' => 'eq',
                'values_from_set' => ['value1', 'value2'],
            ],
        );

        self::assertSame('columnName', $whereFilterFromSet->getColumn());
        self::assertSame('eq', $whereFilterFromSet->getOperator());
        self::assertSame(['value1', 'value2'], $whereFilterFromSet->getValues());
    }
}
