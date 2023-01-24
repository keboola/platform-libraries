<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration;

use Keboola\OutputMapping\Configuration\TableFile;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class TableFileTest extends TestCase
{
    public function testConfiguration(): void
    {
        $config = [
            'tags' => ['tag1', 'tag2'],
        ];
        $expectedResponse = [
            'is_permanent' => true,
            'tags' => ['tag1', 'tag2'],
        ];
        $processedConfiguration = (new TableFile())->parse(['config' => $config]);
        self::assertEquals($expectedResponse, $processedConfiguration);
    }

    public function testInvalidConfiguration(): void
    {
        $config = [
            'tags' => ['tag1', 'tag2'],
            'is_public' => true,
        ];
        self::expectException(InvalidConfigurationException::class);
        (new TableFile())->parse(['config' => $config]);
    }
}
