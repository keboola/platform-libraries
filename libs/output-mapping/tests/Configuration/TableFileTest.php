<?php

namespace Keboola\OutputMapping\Tests\Configuration;

use Keboola\OutputMapping\Configuration\TableFile;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class TableFileTest extends \PHPUnit_Framework_TestCase
{

    public function testConfiguration()
    {
        $config = [
            'tags' => ['tag1', 'tag2'],
        ];
        $expectedResponse = [
            'is_permanent' => true,
            'tags' => ['tag1', 'tag2'],
        ];
        $processedConfiguration = (new TableFile())->parse(['config' => $config]);
        $this->assertEquals($expectedResponse, $processedConfiguration);
    }

    public function testInvalidConfiguration()
    {
        $config = [
            'tags' => ['tag1', 'tag2'],
            'is_public' => true,
        ];
        self::expectException(InvalidConfigurationException::class);
        (new TableFile())->parse(['config' => $config]);
    }
}
