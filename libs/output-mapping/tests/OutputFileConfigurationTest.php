<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\OutputMapping\Configuration\File;

class OutputFileConfigurationTest extends \PHPUnit_Framework_TestCase
{

    public function testConfiguration()
    {
        $config = [
                "source" => "file",
                "tags" => ["tag1", "tag2"]
        ];
        $expectedResponse = [
            "source" => "file",
            "is_public" => false,
            "is_permanent" => false,
            "is_encrypted" => true,
            "notify" => false,
            "tags" => ["tag1", "tag2"]
        ];
        $processedConfiguration = (new File())->parse(["config" => $config]);
        $this->assertEquals($expectedResponse, $processedConfiguration);
    }
}
