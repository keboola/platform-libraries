<?php

namespace Keboola\InputMapping\Tests\Helper;

use Keboola\InputMapping\Helper\LoadTypeDecider;

class LoadTypeDeciderTest extends \PHPUnit_Framework_TestCase
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
        self::assertEquals(LoadTypeDecider::canClone($tableInfo, $workspaceType, $exportOptions), $expected);
    }

    public function decideProvider()
    {
        return [
            'Different Backends' => [
                [
                    'id' => 'foo.bar',
                    'name' => 'bar',
                    'bucket' => ['backend' => 'redshift'],
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
                ],
                'redshift',
                [],
                false,
            ],
        ];
    }
}
