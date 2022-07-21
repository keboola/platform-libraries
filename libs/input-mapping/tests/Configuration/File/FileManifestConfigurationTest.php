<?php

namespace Keboola\InputMapping\Tests\Configuration\File;

use Keboola\InputMapping\Configuration\File\Manifest;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

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

    public function testEmptyConfiguration()
    {
        self::expectException(InvalidConfigurationException::class);
        self::expectExceptionMessage('The child config "id" under "file" must be configured.');
        (new Manifest())->parse(["config" => []]);
    }
}
