<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer;

use Generator;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;
use Keboola\OutputMapping\Writer\Helper\DescriptionHelper;

class TableDescriptionWriterTest extends AbstractTestCase
{
    /** the dedicated `description` field of the mapping */
    private const SOURCE_DEDICATED_FIELD = 'description';

    /** legacy `KBC.description` in the `table_metadata` key => value map */
    private const SOURCE_TABLE_METADATA = 'table_metadata';

    /** legacy `KBC.description` in the `metadata` list of {key, value} items */
    private const SOURCE_METADATA_LIST = 'metadata';

    /**
     * The description reaches Storage the same way regardless of which of the three configuration shapes
     * carried it, and Storage is the only writer of the mirrored `KBC.description` metadata row.
     *
     * @dataProvider descriptionSourceProvider
     */
    #[NeedsEmptyOutputBucket]
    public function testDescriptionIsStoredOnCreatedTable(string $descriptionSource): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDescription';

        $this->uploadTable($tableId, 'table description', 'Id description', $descriptionSource);

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        // `columns` in the mapping creates the table through a non-typed table-definition payload; which
        // branch of LoadTableTaskCreator runs depends on the project features, so pin it here
        self::assertFalse($tableDetail['isTyped']);
        self::assertSame('table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('Id description', $this->getColumnDescription($tableDetail, 'Id'));

        $this->assertSingleStorageDescriptionRow($tableDetail['metadata'], 'table description');
    }

    public static function descriptionSourceProvider(): Generator
    {
        yield 'dedicated description field' => ['descriptionSource' => self::SOURCE_DEDICATED_FIELD];
        yield 'table_metadata KBC.description' => ['descriptionSource' => self::SOURCE_TABLE_METADATA];
        yield 'metadata list KBC.description' => ['descriptionSource' => self::SOURCE_METADATA_LIST];
    }

    /**
     * A table written without a manifest and without `columns` is created by the load job itself
     * (CreateAndLoadTableTask). There is no create-table-definition payload the description could be part of,
     * so it is stored once the load finishes and the table surely exists.
     */
    #[NeedsEmptyOutputBucket]
    public function testDescriptionIsStoredOnTableCreatedByLoadJob(): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDescriptionNoManifest';

        $this->uploadTableWithoutColumns($tableId, 'table description', 'Id description');

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertSame(['Id', 'Name'], $tableDetail['columns']);
        self::assertSame('table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('Id description', $this->getColumnDescription($tableDetail, 'Id'));
    }

    /**
     * The second run finds the table already there, so the description goes through StoragePreparer, which
     * diffs it against the stored value read from `definition.description`. A table created by the load job
     * must expose the description in the same place as one created through a table definition, otherwise the
     * diff always sees null and Storage rejects the unchanged patch with 400.
     */
    #[NeedsEmptyOutputBucket]
    public function testRepeatedRunOnTableCreatedByLoadJobSucceeds(): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDescriptionNoManifest';

        $this->uploadTableWithoutColumns($tableId, 'table description', 'Id description');
        $this->uploadTableWithoutColumns($tableId, 'table description', 'Id description');

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertSame('table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('Id description', $this->getColumnDescription($tableDetail, 'Id'));
    }

    #[NeedsEmptyOutputBucket]
    public function testDescriptionIsUpdatedOnTableCreatedByLoadJob(): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDescriptionNoManifest';

        $this->uploadTableWithoutColumns($tableId, 'table description', 'Id description');
        $this->uploadTableWithoutColumns($tableId, 'updated table description', 'updated Id description');

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertTrue($tableDetail['isDescriptionSystemManaged']);
        self::assertSame('updated table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('updated Id description', $this->getColumnDescription($tableDetail, 'Id'));
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

        self::assertTrue($this->testHandler->hasInfoThatContains(sprintf(
            'Description of table "%s" is managed by the user, keeping the current value.',
            $tableId,
        )));
    }

    /**
     * An empty description means "nothing to store", so a run which no longer carries one keeps the value
     * already in Storage instead of clearing it.
     *
     * @dataProvider descriptionRemovedProvider
     */
    #[NeedsEmptyOutputBucket]
    public function testDescriptionIsKeptWhenNoLongerInConfiguration(
        ?string $tableDescription,
        ?string $columnDescription,
    ): void {
        $tableId = $this->emptyOutputBucketId . '.tableDescription';

        $this->uploadTable($tableId, 'table description', 'Id description');
        $this->uploadTable($tableId, $tableDescription, $columnDescription);

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertTrue($tableDetail['isDescriptionSystemManaged']);
        self::assertSame('table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('Id description', $this->getColumnDescription($tableDetail, 'Id'));
    }

    public static function descriptionRemovedProvider(): Generator
    {
        yield 'description dropped from the configuration' => [
            'tableDescription' => null,
            'columnDescription' => null,
        ];
        yield 'description sent as an empty string' => [
            'tableDescription' => '',
            'columnDescription' => '',
        ];
    }

    /**
     * A description of a column the data does not have must never create that column - it is reported and
     * skipped, and the rest of the load goes through.
     */
    #[NeedsEmptyOutputBucket]
    public function testDescriptionOfMissingColumnIsSkippedWithWarning(): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDescription';

        $this->uploadTable($tableId, 'table description', 'Id description', self::SOURCE_DEDICATED_FIELD, [
            'nope' => [
                ['key' => DescriptionHelper::DESCRIPTION_METADATA_KEY, 'value' => 'description of nothing'],
            ],
        ]);

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertSame(['Id', 'Name'], $tableDetail['columns']);
        self::assertSame('table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('Id description', $this->getColumnDescription($tableDetail, 'Id'));

        self::assertTrue($this->testHandler->hasWarningThatContains(
            'Cannot store description of column(s) "nope"',
        ));
    }

    /**
     * A description in the create-table-definition payload makes Storage write the mirrored
     * `KBC.description` metadata row at create time, before anything is loaded. That row must not be mistaken
     * for "the table has already been used", otherwise a failed load leaves an empty broken table behind.
     */
    #[NeedsEmptyOutputBucket]
    public function testFailedLoadDropsFreshlyCreatedTableWithDescription(): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDescriptionFailedLoad';

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDescriptionFailedLoad.csv',
            "\"test\",\"test\"\n\"aabb\",\"ccdd\",\"dddd\"\n",
        );

        $tableQueue = $this->getTableLoader(logger: $this->testLogger)->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'tableDescriptionFailedLoad.csv',
                            'destination' => $tableId,
                            'columns' => ['Id', 'Name'],
                            'description' => 'table description',
                            'column_metadata' => [
                                'Id' => [
                                    [
                                        'key' => DescriptionHelper::DESCRIPTION_METADATA_KEY,
                                        'value' => 'Id description',
                                    ],
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

        try {
            $tableQueue->waitForAll();
            self::fail('Must throw exception');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString(sprintf('Failed to load table "%s"', $tableId), $e->getMessage());
        }

        self::assertFalse($this->clientWrapper->getTableAndFileStorageClient()->tableExists($tableId));
        self::assertTrue($this->testHandler->hasWarningThatContains(
            sprintf('Failed to load table "%s". Dropping table.', $tableId),
        ));
        // the description was never stored, but it was not lost by mistake either
        self::assertFalse($this->testHandler->hasWarningThatContains('was not stored'));
    }

    /**
     * A table which existed before the failed load keeps its data and its description - only a table freshly
     * created by the very same run may be dropped.
     */
    #[NeedsEmptyOutputBucket]
    public function testFailedLoadKeepsPreExistingTableWithDescription(): void
    {
        $tableId = $this->emptyOutputBucketId . '.tableDescription';

        $this->uploadTable($tableId, 'table description', 'Id description');

        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDescription.csv',
            "\"test\",\"test\"\n\"aabb\",\"ccdd\",\"dddd\"\n",
        );

        $tableQueue = $this->getTableLoader(logger: $this->testLogger)->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'tableDescription.csv',
                            'destination' => $tableId,
                            'columns' => ['Id', 'Name'],
                            'description' => 'table description',
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

        try {
            $tableQueue->waitForAll();
            self::fail('Must throw exception');
        } catch (InvalidOutputException $e) {
            self::assertStringContainsString(sprintf('Failed to load table "%s"', $tableId), $e->getMessage());
        }

        self::assertTrue($this->clientWrapper->getTableAndFileStorageClient()->tableExists($tableId));
        self::assertFalse($this->testHandler->hasWarningThatContains('Dropping table'));

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertSame('table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('Id description', $this->getColumnDescription($tableDetail, 'Id'));
    }

    /**
     * @param array<string, mixed>|null $extraColumnMetadata
     */
    private function uploadTable(
        string $tableId,
        ?string $tableDescription,
        ?string $columnDescription,
        string $descriptionSource = self::SOURCE_DEDICATED_FIELD,
        ?array $extraColumnMetadata = null,
    ): void {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/tableDescription.csv', "\"1\",\"bob\"\n\"2\",\"alice\"\n");

        $mapping = [
            'source' => 'tableDescription.csv',
            'destination' => $tableId,
            'columns' => ['Id', 'Name'],
        ];

        if ($tableDescription !== null) {
            $mapping += match ($descriptionSource) {
                self::SOURCE_DEDICATED_FIELD => ['description' => $tableDescription],
                self::SOURCE_TABLE_METADATA => [
                    'table_metadata' => [
                        DescriptionHelper::DESCRIPTION_METADATA_KEY => $tableDescription,
                    ],
                ],
                self::SOURCE_METADATA_LIST => [
                    'metadata' => [
                        [
                            'key' => DescriptionHelper::DESCRIPTION_METADATA_KEY,
                            'value' => $tableDescription,
                        ],
                    ],
                ],
                default => self::fail(sprintf('Unknown description source "%s".', $descriptionSource)),
            };
        }

        $columnMetadata = $extraColumnMetadata ?? [];
        if ($columnDescription !== null) {
            $columnMetadata['Id'] = [
                ['key' => DescriptionHelper::DESCRIPTION_METADATA_KEY, 'value' => $columnDescription],
            ];
        }
        if ($columnMetadata !== []) {
            $mapping['column_metadata'] = $columnMetadata;
        }

        $tableQueue = $this->getTableLoader(logger: $this->testLogger)->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$mapping]],
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

    private function uploadTableWithoutColumns(
        string $tableId,
        ?string $tableDescription,
        ?string $columnDescription,
    ): void {
        $root = $this->temp->getTmpFolder();
        file_put_contents(
            $root . '/upload/tableDescriptionNoManifest.csv',
            "\"Id\",\"Name\"\n\"1\",\"bob\"\n\"2\",\"alice\"\n",
        );

        $mapping = [
            'source' => 'tableDescriptionNoManifest.csv',
            'destination' => $tableId,
        ];
        if ($tableDescription !== null) {
            $mapping['description'] = $tableDescription;
        }
        if ($columnDescription !== null) {
            $mapping['column_metadata'] = [
                'Id' => [
                    ['key' => DescriptionHelper::DESCRIPTION_METADATA_KEY, 'value' => $columnDescription],
                ],
            ];
        }

        $tableQueue = $this->getTableLoader(logger: $this->testLogger)->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$mapping]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: OutputMappingSettings::DATA_TYPES_SUPPORT_NONE,
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        self::assertCount(1, $tableQueue->waitForAll());
    }

    /**
     * Output mapping stores the description natively and Storage mirrors it into metadata. A second row, or a
     * row under any other provider, would mean output mapping wrote the key itself.
     *
     * @param array<mixed> $metadata
     */
    private function assertSingleStorageDescriptionRow(array $metadata, string $expectedDescription): void
    {
        $descriptionRows = array_values(array_filter(
            $metadata,
            fn($row) => is_array($row) && ($row['key'] ?? null) === DescriptionHelper::DESCRIPTION_METADATA_KEY,
        ));

        self::assertCount(1, $descriptionRows);
        self::assertIsArray($descriptionRows[0]);
        self::assertSame('storage', $descriptionRows[0]['provider']);
        self::assertSame($expectedDescription, $descriptionRows[0]['value']);
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
