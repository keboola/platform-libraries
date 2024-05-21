<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Generator;
use Keboola\Datatype\Definition\Common;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyInputBucket;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Writer\TableWriter;

class TableDefinitionV2Test extends AbstractTestCase
{
    public function setUp(): void
    {
        parent::setUp();

        $requiredFeatures = [
            'new-native-types',
        ];

        $tokenData = $this->clientWrapper->getBranchClient()->verifyToken();
        foreach ($requiredFeatures as $requiredFeature) {
            if (!in_array($requiredFeature, $tokenData['owner']['features'])) {
                self::fail(sprintf(
                    '%s is not enabled for project "%s".',
                    ucfirst(str_replace('-', ' ', $requiredFeature)),
                    $tokenData['owner']['id'],
                ));
            }
        }
    }

    /**
     * @dataProvider conflictsConfigurationWithManifestProvider
     */
    public function testConflictSchemaManifestAndNonSchemaConfiguration(
        array $config,
        string $expectedErrorMessage,
    ): void {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDefinition.csv',
            "\"Id\",\"Name\"\n\"test\",\"test\"\n\"aabb\",\"ccdd\"\n",
        );

        file_put_contents(
            $root . '/upload/tableDefinition.csv.manifest',
            json_encode([
                'schema' => [
                    [
                        'name' => 'Id',
                        'primary_key' => true,
                        'data_type' => [
                            'base' => [
                                'type' => 'int',
                            ],
                        ],
                    ],
                    [
                        'name' => 'Name',
                        'data_type' => [
                            'base' => [
                                'type' => 'string',
                            ],
                        ],
                    ],
                ],
            ]),
        );

        $baseConfig = [
            'source' => 'tableDefinition.csv',
            'destination' => 'in.c-test.tableDefinition',
        ];

        $writer = new TableWriter($this->getLocalStagingFactory(logger: $this->testLogger));

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage($expectedErrorMessage);
        $writer->uploadTables(
            'upload',
            ['mapping' => [array_merge($baseConfig, $config)]],
            ['componentId' => 'foo'],
            'local',
            false,
            'none',
        );
    }

    public function conflictsConfigurationWithManifestProvider(): Generator
    {
        yield 'conflict-columns' => [
            [
                'columns' => ['Id', 'Name'],
            ],
            'Only one of "schema" or "columns" can be defined.',
        ];

        yield 'conflict-primary_keys' => [
            [
                'primary_key' => ['Id', 'Name'],
            ],
            'Only one of "primary_key" or "schema[].primary_key" can be defined.',
        ];

        yield 'conflict-distribution_key' => [
            [
                'distribution_key' => ['Id', 'Name'],
            ],
            'Only one of "distribution_key" or "schema[].distribution_key" can be defined.',
        ];

        yield 'conflict-metadata' => [
            [
                'metadata' => [
                    [
                        'key' => 'table.key.one',
                        'value' => 'table value one',
                    ],
                    [
                        'key' => 'table.key.two',
                        'value' => 'table value two',
                    ],
                ],
            ],
            'Only one of "schema" or "metadata" can be defined.',
        ];

        yield 'conflict-column_metadata' => [
            [
                'column_metadata' => [
                    'Id' => [
                        [
                            'key' => 'KBC.dataType',
                            'value' => 'VARCHAR',
                        ],
                    ],
                ],
            ],
            'Only one of "schema" or "column_metadata" can be defined.',
        ];
    }
}
