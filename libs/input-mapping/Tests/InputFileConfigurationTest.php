<?php

namespace Keboola\InputMapping\Tests;

use Keboola\InputMapping\Configuration\File;

class InputFileConfigurationTest extends \PHPUnit_Framework_TestCase
{
    public function testConfiguration()
    {
        $config = [
            "tags" => ["tag1", "tag2"],
            "query" => "esquery",
            "processed_tags" => ["tag3"],
            "filter_by_run_id" => true,
            "limit" => 1000
        ];
        $expectedResponse = $config;
        $processedConfiguration = (new File())->parse(["config" => $config]);
        self::assertEquals($expectedResponse, $processedConfiguration);
    }

    /**
     * @expectedException \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException
     * @expectedExceptionMessage Invalid configuration for path "file": At least one of 'tags' or 'query' parameters must be defined.
     */
    public function testEmptyConfiguration()
    {
        (new File())->parse(["config" => []]);
    }
}
