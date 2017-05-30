<?php

namespace Keboola\OutputMapping\Tests;

use Keboola\OutputMapping\Configuration\Output\File;

class OutputFileConfigurationTest extends \PHPUnit_Framework_TestCase
{

    public function testConfiguration()
    {
        $config = array(
                "source" => "file",
                "tags" => array("tag1", "tag2")
            );
        $expectedResponse = array(
            "source" => "file",
            "is_public" => false,
            "is_permanent" => false,
            "is_encrypted" => true,
            "notify" => false,
            "tags" => array("tag1", "tag2")
        );
        $processedConfiguration = (new File())->parse(array("config" => $config));
        $this->assertEquals($expectedResponse, $processedConfiguration);
    }
}
