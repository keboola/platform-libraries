<?php

namespace Keboola\InputMapping\Tests\Configuration\File;

use Keboola\InputMapping\Configuration\File\Manifest;

class FileManifestConfigurationTest extends \PHPUnit_Framework_TestCase
{
    public function testConfiguration()
    {
        $config = [
            "id" => 1,
            "name" => "test",
            "created" => "2015-01-23T04:11:18+0100",
            "is_public" => false,
            "is_encrypted" => false,
            "tags" => ["tag1", "tag2"],
            "max_age_days" => 180,
            "size_bytes" => 4,
            'is_sliced' => false,
        ];
        $expectedResponse = $config;
        $processedConfiguration = (new Manifest())->parse(["config" => $config]);
        self::assertEquals($expectedResponse, $processedConfiguration);
    }

    /**
     * @expectedException \Symfony\Component\Config\Definition\Exception\InvalidConfigurationException
     * @expectedExceptionMessage The child node "id" at path "file" must be configured.
     */
    public function testEmptyConfiguration()
    {
        (new Manifest())->parse(["config" => []]);
    }
}
