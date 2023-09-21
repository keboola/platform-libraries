<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Configuration\Table;

use Generator;
use Keboola\InputMapping\Configuration\Table\Manifest;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class TableManifestConfigurationTest extends TestCase
{
    public function configurationData(): Generator
    {
        yield 'columns' => [
            'primaryKey' => ['col1', 'col2'],
            'distributionKey' => ['col1'],
            'columns' => ['col1', 'col2', 'col3', 'col4'],
            'columnsMetadata' => ['col1' => [['key' => 'bar', 'value' => 'baz']]],
        ];
        yield 'numeric columns' => [
            'primaryKey' => ['0', '1'],
            'distributionKey' => ['0'],
            'columns' => ['0', '1', '2', '3'],
            'columnsMetadata' => ['0' => [['key' => 'bar', 'value' => 'baz']]],
        ];
    }

    /**
     * @dataProvider configurationData
     */
    public function testConfiguration(
        array $primaryKey,
        array $distributionKey,
        array $columns,
        array $columnsMetadata,
    ): void {
        $config = [
            'id' => 'in.c-docker-test.test',
            'uri' => 'https://connection.keboola.com//v2/storage/tables/in.c-docker-test.test',
            'name' => 'test',
            'primary_key' => $primaryKey,
            'distribution_key' => $distributionKey,
            'created' => '2015-01-23T04:11:18+0100',
            'last_import_date' => '2015-01-23T04:11:18+0100',
            'last_change_date' => '2015-01-23T04:11:18+0100',
            'columns' => $columns,
            'metadata' => [[
                'key' => 'foo',
                'value' => 'bar',
                'id' => 1234,
                'provider' => 'dummy-component',
                'timestamp' => '2017-05-25T16:12:02+0200',
            ]],
            'column_metadata' => $columnsMetadata,
        ];
        $expectedResponse = $config;
        $processedConfiguration = (new Manifest())->parse(['config' => $config]);

        self::assertEquals($expectedResponse, $processedConfiguration);
    }

    public function testEmptyConfiguration(): void
    {
        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The child config "id" under "table" must be configured.');
        (new Manifest())->parse(['config' => []]);
    }
}
