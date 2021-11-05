<?php

declare(strict_types=1);

namespace Keboola\ConfigurationVariablesResolver\Tests\Configuration;

use Keboola\ConfigurationVariablesResolver\Configuration\SharedCodeRow;
use PHPUnit\Framework\TestCase;

class SharedCodeRowConfigurationTest extends TestCase
{
    public function testSharedCodeRowConfiguration(): void
    {
        $configuration = [
            'variables_id' => 123,
            'code_content' => ['some {{script}} line 1', '{{some}} script line 2 '],
        ];
        $result = (new SharedCodeRow())->process($configuration);
        self::assertEquals($configuration, $result);
    }

    public function testSharedCodeRowConfigurationStringToArray(): void
    {
        $expectedResult = [
            'variables_id' => 123,
            'code_content' => ['some script'],
        ];
        $config = (new SharedCodeRow())->process([
            'variables_id' => 123,
            'code_content' => 'some script',
        ]);
        self::assertEquals($expectedResult, $config);
    }
}
