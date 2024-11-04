<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Storage\StoragePreparer;
use Keboola\OutputMapping\Storage\TableChangesStore;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyBigqueryOutputBucket;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Util\Test;

class BigqueryStoragePreparerTest extends AbstractTestCase
{
    #[NeedsEmptyBigqueryOutputBucket]
    public function testPrepareStorageBucketAndTableWithNewNativeTypes(): void
    {
        $storagePreparer = new StoragePreparer(
            $this->clientWrapper,
            $this->testLogger,
            hasNewNativeTypeFeature: true,
            hasBigQueryNativeTypesFeature: false,
        );

        $tableId = $this->createTestTypedTable($this->emptyBigqueryOutputBucketId);

        $newColumnName = 'col2';

        $changesStore = new TableChangesStore();
        $changesStore->addMissingColumn(new MappingFromConfigurationSchemaColumn([
            'name' => $newColumnName,
            'data_type' => [
                'base' => [
                    'type' => 'NUMERIC',
                ],
                'bigquery' => [
                    'type' => 'NUMERIC',
                    'length' => '10,5',
                ],
            ],
        ]));

        $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration(
                $this->createMappingData($tableId, $newColumnName, false),
            ),
            $this->createSystemMetadata(),
            $changesStore,
        );

        $newColumnDefinition = $this->getTableColumnDefinition($tableId, $newColumnName);
        self::assertNotNull($newColumnDefinition);

        self::assertSame(
            [
                'name' => $newColumnName,
                'definition' => [
                    'type' => 'NUMERIC',
                    'nullable' => true,
                    'length' => '10,5',
                ],
                'basetype' => 'NUMERIC',
                'canBeFiltered' => true,
            ],
            $newColumnDefinition,
        );
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testPrepareStorageBucketAndTableWithoutEnforcedBaseTypes(): void
    {
        $storagePreparer = new StoragePreparer(
            $this->clientWrapper,
            $this->testLogger,
            hasNewNativeTypeFeature: false,
            hasBigQueryNativeTypesFeature: true,
        );

        $tableId = $this->createTestTypedTable($this->emptyBigqueryOutputBucketId);

        $newColumnName = 'col2';

        $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration(
                $this->createMappingData($tableId, $newColumnName),
            ),
            $this->createSystemMetadata(),
            new TableChangesStore(),
        );

        $newColumnDefinition = $this->getTableColumnDefinition($tableId, $newColumnName);
        self::assertNotNull($newColumnDefinition);

        self::assertSame(
            [
                'name' => $newColumnName,
                'definition' => [
                    'type' => 'NUMERIC',
                    'nullable' => true,
                    'length' => '10,5',
                ],
                'basetype' => 'NUMERIC',
                'canBeFiltered' => true,
            ],
            $newColumnDefinition,
        );
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testPrepareStorageBucketAndTableWithEnforcedBaseTypes(): void
    {
        $storagePreparer = new StoragePreparer(
            $this->clientWrapper,
            $this->testLogger,
            hasNewNativeTypeFeature: false,
            hasBigQueryNativeTypesFeature: false,
        );

        $tableId = $this->createTestTypedTable($this->emptyBigqueryOutputBucketId);

        $newColumnName = 'col2';

        $storagePreparer->prepareStorageBucketAndTable(
            $this->createMappingFromProcessedConfiguration(
                $this->createMappingData($tableId, $newColumnName),
            ),
            $this->createSystemMetadata(),
            new TableChangesStore(),
        );

        $newColumnDefinition = $this->getTableColumnDefinition($tableId, $newColumnName);
        self::assertNotNull($newColumnDefinition);

        self::assertSame(
            [
                'name' => $newColumnName,
                'definition' => [
                    'type' => 'NUMERIC',
                    'nullable' => true,
                ],
                'basetype' => 'NUMERIC',
                'canBeFiltered' => true,
            ],
            $newColumnDefinition,
        );
    }

    protected function initClient(?string $branchId = null): void
    {
        $clientOptions = (new ClientOptions())
            ->setUrl((string) getenv('BIGQUERY_STORAGE_API_URL'))
            ->setToken((string) getenv('BIGQUERY_STORAGE_API_TOKEN'))
            ->setBranchId($branchId)
            ->setBackoffMaxTries(1)
            ->setJobPollRetryDelay(function () {
                return 1;
            })
            ->setUserAgent(implode('::', Test::describe($this)));
        $this->clientWrapper = new ClientWrapper($clientOptions);
        $tokenInfo = $this->clientWrapper->getBranchClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $this->clientWrapper->getBranchClient()->getApiUrl(),
        ));
    }

    private function createSystemMetadata(): SystemMetadata
    {
        return new SystemMetadata([
            'componentId' => 'keboola.output-mapping',
        ]);
    }

    private function createTestTypedTable(string $bucketId): string
    {
        return $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition($bucketId, [
            'name' => 'test1',
            'primaryKeysNames' => [],
            'columns' => [
                [
                    'name' => 'col1',
                    'basetype' => 'STRING',
                ],
            ],
        ]);
    }

    private function createMappingData(string $tableId, string $newColumnName, bool $legacyManifest = true): array
    {
        if ($legacyManifest) {
            return [
                'destination' => $tableId,
                'delimiter' => ',',
                'enclosure' => '"',
                'columns' => [
                    'col1',
                    $newColumnName,
                ],
                'metadata' => [
                    [
                        'key' => 'KBC.datatype.backend',
                        'value' => 'bigquery',
                    ],
                ],
                'column_metadata' => [
                    'col1' => [
                        [
                            'key' => 'KBC.datatype.type',
                            'value' => 'STRING',
                        ],
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ],
                    ],
                    $newColumnName => [
                        [
                            'key' => 'KBC.datatype.type',
                            'value' => 'NUMERIC',
                        ],
                        [
                            'key' => 'KBC.datatype.length',
                            'value' => '10,5',
                        ],
                        [
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'NUMERIC',
                        ],
                    ],
                ],
            ];
        } else {
            return [
                'destination' => $tableId,
                'delimiter' => ',',
                'enclosure' => '"',
                'schema' => [
                    [
                        'name' => 'col1',
                        'data_type' => [
                            'base' => [
                                'type' => 'STRING',
                            ],
                        ],
                    ],
                    [
                        'name' => $newColumnName,
                        'data_type' => [
                            'base' => [
                                'type' => 'NUMERIC',
                            ],
                            'bigquery' => [
                                'type' => 'NUMERIC',
                                'length' => '10,5',
                            ],
                        ],
                    ],
                ],
            ];
        }
    }

    private function createMappingFromProcessedConfiguration(array $mapping): MappingFromProcessedConfiguration
    {
        $physicalDataWithManifest = $this->createMock(
            MappingFromRawConfigurationAndPhysicalDataWithManifest::class,
        );

        return new MappingFromProcessedConfiguration($mapping, $physicalDataWithManifest);
    }

    private function getTableColumnDefinition(string $tableId, string $columnName): ?array
    {
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        foreach ($table['definition']['columns'] as $column) {
            if ($column['name'] === $columnName) {
                return $column;
            }
        }

        return null;
    }
}
