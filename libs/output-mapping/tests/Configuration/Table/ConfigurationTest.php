<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration\Table;

use Keboola\OutputMapping\Configuration\Table;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigurationTest extends ManifestTest
{
    public function testBasicConfiguration(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'source' => 'in.c-main.source',
        ];

        $expectedArray = [
            'destination' => 'in.c-main.test',
            'primary_key' => [],
            'distribution_key' => [],
            'columns' => [],
            'incremental' => false,
            'delete_where_values' => [],
            'delete_where_operator' => 'eq',
            'delimiter' => ',',
            'enclosure' => '"',
            'metadata' => [],
            'column_metadata' => [],
            'write_always' => false,
            'tags' => [],
            'source' => 'in.c-main.source',
        ];

        $processedConfiguration = (new Table\Configuration())->parse(['config' => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }
}
