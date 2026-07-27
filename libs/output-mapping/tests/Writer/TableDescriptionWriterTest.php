<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;

class TableDescriptionWriterTest extends AbstractTestCase
{
    #[NeedsEmptyOutputBucket]
    public function testDescriptionIsStoredOnCreatedTable(): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDescription';

        $this->uploadTable($tableId, 'table description', 'Id description');

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertSame('table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('Id description', $this->getColumnDescription($tableDetail, 'Id'));
    }

    #[NeedsEmptyOutputBucket]
    public function testDescriptionIsUpdatedOnSystemManagedTable(): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDescription';

        $this->uploadTable($tableId, 'table description', 'Id description');
        $this->uploadTable($tableId, 'updated table description', 'updated Id description');

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertTrue($tableDetail['isDescriptionSystemManaged']);
        self::assertSame('updated table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('updated Id description', $this->getColumnDescription($tableDetail, 'Id'));
    }

    /**
     * Storage rejects a table-definition patch without any effective change with 400 "No table definition
     * changes were provided.", so a repeated run with an unchanged description must not send the patch at all.
     */
    #[NeedsEmptyOutputBucket]
    public function testRepeatedRunWithUnchangedDescriptionSucceeds(): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDescription';

        $this->uploadTable($tableId, 'table description', 'Id description');
        $this->uploadTable($tableId, 'table description', 'Id description');

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertSame('table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('Id description', $this->getColumnDescription($tableDetail, 'Id'));
    }

    #[NeedsEmptyOutputBucket]
    public function testDescriptionIsNotOverwrittenOnUserManagedTable(): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDescription';

        $this->uploadTable($tableId, 'table description', 'Id description');

        // the user takes over the description
        $this->clientWrapper->getTableAndFileStorageClient()->updateTableDefinition($tableId, [
            'description' => 'description set by the user',
            'isDescriptionSystemManaged' => false,
            'columns' => [
                ['name' => 'Id', 'description' => 'Id description set by the user'],
            ],
        ]);

        $this->uploadTable($tableId, 'table description from component', 'Id description from component');

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertFalse($tableDetail['isDescriptionSystemManaged']);
        self::assertSame('description set by the user', $tableDetail['definition']['description'] ?? null);
        self::assertSame('Id description set by the user', $this->getColumnDescription($tableDetail, 'Id'));
    }

    private function uploadTable(string $tableId, string $tableDescription, string $columnDescription): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/tableDescription.csv', "\"1\",\"bob\"\n\"2\",\"alice\"\n");

        $tableQueue = $this->getTableLoader()->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'tableDescription.csv',
                            'destination' => $tableId,
                            'columns' => ['Id', 'Name'],
                            'description' => $tableDescription,
                            'column_metadata' => [
                                'Id' => [
                                    ['key' => 'KBC.description', 'value' => $columnDescription],
                                ],
                            ],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: OutputMappingSettings::DATA_TYPES_SUPPORT_NONE,
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        $jobIds = $tableQueue->waitForAll();
        self::assertCount(1, $jobIds);
    }

    private function getColumnDescription(array $tableDetail, string $columnName): ?string
    {
        $definition = $tableDetail['definition'] ?? [];
        self::assertIsArray($definition);
        $columns = $definition['columns'] ?? [];
        self::assertIsArray($columns);

        foreach ($columns as $column) {
            self::assertIsArray($column);
            if ($column['name'] === $columnName) {
                $columnDefinition = $column['definition'] ?? [];
                self::assertIsArray($columnDefinition);

                return $columnDefinition['description'] ?? null;
            }
        }

        return null;
    }
}
