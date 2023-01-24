<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Configuration\File;

use Keboola\InputMapping\Configuration\File\Manifest;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class FileManifestConfigurationTest extends TestCase
{
    public function testConfiguration(): void
    {
        $config = [
            'id' => 1,
            'name' => 'test',
            'created' => '2015-01-23T04:11:18+0100',
            'is_public' => false,
            'is_encrypted' => false,
            'tags' => ['tag1', 'tag2'],
            'max_age_days' => 180,
            'size_bytes' => 4,
            'is_sliced' => false,
        ];
        $expectedResponse = $config;
        $processedConfiguration = (new Manifest())->parse(['config' => $config]);
        self::assertEquals($expectedResponse, $processedConfiguration);
    }

    public function testEmptyConfiguration(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The child config "id" under "file" must be configured.');
        (new Manifest())->parse(['config' => []]);
    }
}
