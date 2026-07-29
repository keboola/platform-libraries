<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Storage;

use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyBigqueryOutputBucket;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Util\Test;

/**
 * Descriptions are stored through the table-definition API on every backend, so BigQuery has to accept both
 * the `description` of the create payload and the table-definition update of a following run.
 */
class BigqueryTableDescriptionTest extends AbstractTestCase
{
    #[NeedsEmptyBigqueryOutputBucket]
    public function testDescriptionIsStoredOnCreatedTable(): void
    {
        $tableId = $this->emptyBigqueryOutputBucketId . '.tableDescription';

        $this->uploadTable($tableId, 'table description', 'Id description');

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertSame('table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('Id description', $this->getColumnDescription($tableDetail, 'Id'));
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testDescriptionIsUpdatedOnExistingTable(): void
    {
        $tableId = $this->emptyBigqueryOutputBucketId . '.tableDescription';

        $this->uploadTable($tableId, 'table description', 'Id description');
        $this->uploadTable($tableId, 'updated table description', 'updated Id description');

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertTrue($tableDetail['isDescriptionSystemManaged']);
        self::assertSame('updated table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('updated Id description', $this->getColumnDescription($tableDetail, 'Id'));
    }

    /**
     * Storage rejects a table-definition patch without any effective change with 400, so an unchanged repeated
     * run must not send one.
     */
    #[NeedsEmptyBigqueryOutputBucket]
    public function testRepeatedRunWithUnchangedDescriptionSucceeds(): void
    {
        $tableId = $this->emptyBigqueryOutputBucketId . '.tableDescription';

        $this->uploadTable($tableId, 'table description', 'Id description');
        $this->uploadTable($tableId, 'table description', 'Id description');

        $tableDetail = $this->clientWrapper->getTableAndFileStorageClient()->getTable($tableId);
        self::assertSame('table description', $tableDetail['definition']['description'] ?? null);
        self::assertSame('Id description', $this->getColumnDescription($tableDetail, 'Id'));
    }

    private function uploadTable(string $tableId, string $tableDescription, string $columnDescription): void
    {
        $root = $this->temp->getTmpFolder();
        file_put_contents($root . '/upload/tableDescription.csv', "\"1\",\"bob\"\n\"2\",\"alice\"\n");

        $tableQueue = $this->getTableLoader(logger: $this->testLogger)->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => 'tableDescription.csv',
                            'destination' => $tableId,
                            'description' => $tableDescription,
                            'schema' => [
                                [
                                    'name' => 'Id',
                                    'data_type' => ['base' => ['type' => 'STRING']],
                                    'description' => $columnDescription,
                                ],
                                [
                                    'name' => 'Name',
                                    'data_type' => ['base' => ['type' => 'STRING']],
                                ],
                            ],
                        ],
                    ],
                ],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: OutputMappingSettings::DATA_TYPES_SUPPORT_AUTHORITATIVE,
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        self::assertCount(1, $tableQueue->waitForAll());
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

    protected function initClient(?string $branchId = null): void
    {
        $clientOptions = (new ClientOptions())
            ->setUrl((string) getenv('BIGQUERY_STORAGE_API_URL'))
            ->setToken((string) getenv('BIGQUERY_STORAGE_API_TOKEN'))
            ->setAuthType(AuthType::STORAGE_TOKEN)
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
