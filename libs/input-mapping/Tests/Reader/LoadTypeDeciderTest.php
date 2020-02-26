<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\InputMapping\Reader\LoadTypeDecider;

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
            ['a' => 'b']
        ];
    }
}
