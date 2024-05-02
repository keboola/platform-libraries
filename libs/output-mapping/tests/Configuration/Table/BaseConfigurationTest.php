<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration\Table;

use Keboola\OutputMapping\Configuration\Table;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class BaseConfigurationTest extends TestCase
{
    public function testBasicConfiguration(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
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
            'schema' => [],
        ];

        $this->testManifestAndConfig($config, $expectedArray);
    }

    public function testMinimalSchemaConfig(): void
    {
        $config = [
            'destination' => 'in.c-main.test',
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                    ],
                ],
            ],
        ];

        $expectedArray = [
            'destination' => 'in.c-main.test','primary_key' => [],
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
            'schema' => [
                [
                    'name' => 'test',
                    'data_type' => [
                        'base' => [
                            'type' => 'string',
                        ],
                    ],
                    'nullable' => true,
                    'primary_key' => false,
                ],
            ],
        ];

        $this->testManifestAndConfig($config, $expectedArray);
    }

//    public function test()
//    {
//
//    }


    private function testManifestAndConfig(
        array $config,
        array $expectedConfig,
        ?string $expectedErrorMessage = null,
    ): void {
        if ($expectedErrorMessage !== null) {
            try {
                (new Table\Manifest())->parse(['config' => $config]);
                self::fail('Exception should be thrown');
            } catch (InvalidConfigurationException $e) {
                self::assertEquals($expectedErrorMessage, $e->getMessage());
            }
            try {
                (new Table\Configuration())->parse(['config' => $config]);
                self::fail('Exception should be thrown');
            } catch (InvalidConfigurationException $e) {
                self::assertEquals($expectedErrorMessage, $e->getMessage());
            }
        } else {
            self::assertEquals($expectedConfig, (new Table\Manifest())->parse(['config' => $config]));
            self::assertEquals($expectedConfig, (new Table\Configuration())->parse(['config' => $config]));
        }
    }
}
