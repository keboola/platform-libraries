<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Storage\BucketInfo;
use Keboola\OutputMapping\Storage\TableChangesStore;
use Keboola\OutputMapping\Storage\TableInfo;
use Keboola\OutputMapping\Storage\TableStructureModifierFromSchema;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyBigqueryOutputBucket;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Util\Test;

class BigqueryTableStructureModifierFromSchemaTest extends AbstractTestCase
{
    private TableStructureModifierFromSchema $tableStructureModifier;

    private array $bucket;
    private array $table;

    public function setup(): void
    {
        parent::setup();

        $this->tableStructureModifier = new TableStructureModifierFromSchema($this->clientWrapper, $this->testLogger);
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testModifyColumnAttributes(): void
    {
        $this->prepareStorageData();

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->table['id']);
        self::assertEquals(
            [
                'name' => 'Name',
                'definition' => [
                    'type' => 'STRING',
                    'nullable' => false,
                    'length' => '17',
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => true,
            ],
            $table['definition']['columns'][1],
        );

        $tableChangesStore = new TableChangesStore();

        $tableChangesStore->addColumnAttributeChanges(new MappingFromConfigurationSchemaColumn([
            'name' => 'Name',
            'data_type' => [
                'base' => [
                    'type' => 'VARCHAR',
                    'length' => '255',
                    'default' => 'new default value',
                ],
            ],
            'nullable' => true,
        ]));

        $this->tableStructureModifier->updateTableStructure(
            new BucketInfo($this->bucket),
            new TableInfo($this->table),
            $tableChangesStore,
        );

        $updatedTable = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->table['id']);
        self::assertEquals(
            [
                'name' => 'Name',
                'definition' => [
                    'type' => 'STRING',
                    'nullable' => true,
                    'length' => '255',
                    // 'default' => '\'new default value\'',
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => true,
            ],
            $updatedTable['definition']['columns'][1],
        );
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testModifyColumnNullableToFalseError(): void
    {
        $this->prepareStorageData();
        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->table['id']);
        self::assertEquals(
            [
                'name' => 'Id',
                'definition' => [
                    'type' => 'INTEGER',
                    'nullable' => true,
                ],
                'basetype' => 'INTEGER',
                'canBeFiltered' => true,
            ],
            $table['definition']['columns'][0],
        );

        $tableChangesStore = new TableChangesStore();

        $tableChangesStore->addColumnAttributeChanges(new MappingFromConfigurationSchemaColumn([
            'name' => 'Id',
            'data_type' => [
                'base' => [
                    'type' => 'NUMERIC',
                ],
            ],
            'nullable' => false,
        ]));

        try {
            $this->tableStructureModifier->updateTableStructure(
                new BucketInfo($this->bucket),
                new TableInfo($this->table),
                $tableChangesStore,
            );
            $this->fail('UpdateTableStructure should fail with InvalidOutputException');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString('BigQuery column cannot be set as required', $e->getMessage());
        }
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testModifyColumnAttributesError(): void
    {
        $this->markTestSkipped('Default Value is not works correctly');

        // $this->prepareStorageData();
        //
        // $tableChangesStore = new TableChangesStore();
        //
        // $tableChangesStore->addColumnAttributeChanges(new MappingFromConfigurationSchemaColumn([
        //     'name' => 'Id',
        //     'data_type' => [
        //         'base' => [
        //             'type' => 'NUMERIC',
        //             'default' => 'new default value',
        //         ],
        //     ],
        // ]));
        //
        // try {
        //     $this->tableStructureModifier->updateTableStructure(
        //         new BucketInfo($this->bucket),
        //         new TableInfo($this->table),
        //         $tableChangesStore,
        //     );
        //     $this->fail('UpdateTableStructure should fail with InvalidOutputException');
        // } catch (InvalidOutputException $e) {
        //     self::assertStringContainsString(
        //         'Invalid default value for column "Id". Expected numeric value, got "new default value".',
        //         $e->getMessage(),
        //     );
        // }
    }

    private function prepareStorageData(array $primaryKeyNames = []): void
    {
        $idDatatype = new GenericStorage('int', ['nullable' => true]);
        $nameDatatype = new GenericStorage('string', ['length' => '17', 'nullable' => false]);

        $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition(
            $this->emptyBigqueryOutputBucketId,
            [
                'name' => 'tableDefinition',
                'primaryKeysNames' => $primaryKeyNames,
                'columns' => [
                    [
                        'name' => 'Id',
                        'basetype' => $idDatatype->getBasetype(),
                        'definition' => [
                            'type' => $idDatatype->getType(),
                            'nullable' => $idDatatype->isNullable(),
                        ],
                    ],
                    [
                        'name' => 'Name',
                        'basetype' => $nameDatatype->getBasetype(),
                        'definition' => [
                            'type' => $nameDatatype->getType(),
                            'length' => $nameDatatype->getLength(),
                            'nullable' => $nameDatatype->isNullable(),
                        ],
                    ],
                ],
            ],
        );

        $this->bucket = $this
            ->clientWrapper
            ->getTableAndFileStorageClient()
            ->getBucket($this->emptyBigqueryOutputBucketId);

        $this->table = $this
            ->clientWrapper
            ->getTableAndFileStorageClient()
            ->getTable($this->emptyBigqueryOutputBucketId . '.tableDefinition');
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
}
