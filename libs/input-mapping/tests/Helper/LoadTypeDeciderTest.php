<?php

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Helper\LoadTypeDecider;
use PHPUnit\Framework\TestCase;

class LoadTypeDeciderTest extends TestCase
{
    /**
     * @dataProvider decideProvider
     * @param array $tableInfo
     * @param string $workspaceType
     * @param array $exportOptions
     * @param bool $expected
     */
    public function testDecide(array $tableInfo, $workspaceType, array $exportOptions, $expected)
    {
        self::assertEquals($expected, LoadTypeDecider::canClone($tableInfo, $workspaceType, $exportOptions));
    }

    public function decideProvider()
    {
        return [
            'Different Backends' => [
                [
                    'id' => 'foo.bar',
                    'name' => 'bar',
                    'bucket' => ['backend' => 'redshift'],
                    'isAlias' => false,
                ],
                'snowflake',
                [],
                false,
            ],
            'Different Backends 2' => [
                [
                    'id' => 'foo.bar',
                    'name' => 'bar',
                    'bucket' => ['backend' => 'snowflake'],
                    'isAlias' => false,
                ],
                'redshift',
                [],
                false,
            ],
            'Filtered' => [
                [
                    'id' => 'foo.bar',
                    'name' => 'bar',
                    'bucket' => ['backend' => 'snowflake'],
                    'isAlias' => false,
                ],
                'snowflake',
                [
                    'changed_since' => '-2 days',
                ],
                false,
            ],
            'cloneable snowflake' => [
                [
                    'id' => 'foo.bar',
                    'name' => 'bar',
                    'bucket' => ['backend' => 'snowflake'],
                    'isAlias' => false,
                ],
                'snowflake',
                ['overwrite' => false],
                true,
            ],
            'redshift' => [
                [
                    'id' => 'foo.bar',
                    'name' => 'bar',
                    'bucket' => ['backend' => 'redshift'],
                    'isAlias' => false,
                ],
                'redshift',
                [],
                false,
            ],
            'alias table' => [
                [
                    'id' => 'foo.bar',
                    'name' => 'bar',
                    'bucket' => ['backend' => 'snowflake'],
                    'isAlias' => true,
                    'aliasColumnsAutoSync' => true,
                ],
                'snowflake',
                ['overwrite' => false],
                true,
            ],
            'alias filtered columns' => [
                [
                    'id' => 'foo.bar',
                    'name' => 'bar',
                    'bucket' => ['backend' => 'snowflake'],
                    'isAlias' => true,
                    'aliasColumnsAutoSync' => false,
                ],
                'snowflake',
                ['overwrite' => false],
                false,
            ],
            'alias filtered rows' => [
                [
                    'id' => 'foo.bar',
                    'name' => 'bar',
                    'bucket' => ['backend' => 'snowflake'],
                    'isAlias' => true,
                    'aliasColumnsAutoSync' => true,
                    'aliasFilter' => [
                        'column' => 'PassengerId',
                        'operator' => 'eq',
                        'values' => ['12'],
                    ],
                ],
                'snowflake',
                ['overwrite' => false],
                false,
            ],
        ];
    }
}
