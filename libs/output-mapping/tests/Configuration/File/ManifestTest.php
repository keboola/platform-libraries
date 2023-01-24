<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration\File;

use Keboola\OutputMapping\Configuration\File\Manifest;
use PHPUnit\Framework\TestCase;

class ManifestTest extends TestCase
{

    public function testConfiguration(): void
    {
        $config = ['tags' => ['tag1', 'tag2']];
        $expectedResponse = [
            'is_public' => false,
            'is_permanent' => false,
            'is_encrypted' => true,
            'notify' => false,
            'tags' => ['tag1', 'tag2'],
        ];
        $processedConfiguration = (new Manifest())->parse(['config' => $config]);
        self::assertEquals($expectedResponse, $processedConfiguration);
    }
}
