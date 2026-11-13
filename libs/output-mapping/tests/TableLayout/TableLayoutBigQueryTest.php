<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\TableLayout;

use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromConfigurationSchemaColumn;
use Keboola\OutputMapping\Mapping\MappingTableLayout;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyBigqueryOutputBucket;
use Keboola\OutputMapping\Writer\Table\TableDefinitionFromSchema\TableDefinitionFromSchema;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Util\Test;

class TableLayoutBigQueryTest extends AbstractTestCase
{
    private const SOURCE = 'tableLayout.csv';
    private const TABLE_NAME = 'tableLayout';
    private const DIFFERENT_LAYOUT_WARNING = 'already exists with layout';
    private const OTHER_BACKEND_WARNING = 'have no effect on the';

    public function setUp(): void
    {
        parent::setUp();

        $tokenData = $this->clientWrapper->getBranchClient()->verifyToken();
        if (!in_array('new-native-types', $tokenData['owner']['features'])) {
            self::fail(sprintf('New native types is not enabled for project "%s".', $tokenData['owner']['id']));
        }
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testNewTypedTableGetsTimePartitioningAndClustering(): void
    {
        $this->writeHeadlessCsv();

        $this->runOutputMapping(
            [
                'source' => self::SOURCE,
                'destination' => $this->getTableId(),
                'schema' => $this->getSchema(),
                'time_partitioning' => ['type' => 'DAY', 'field' => 'report_date'],
                'clustering' => ['fields' => ['country']],
            ],
            OutputMappingSettings::DATA_TYPES_SUPPORT_AUTHORITATIVE,
        );

        $this->assertTimePartitioningAndClustering();
        self::assertSame(2, $this->getRowsCount());
        self::assertFalse($this->testHandler->hasWarningThatContains(self::OTHER_BACKEND_WARNING));
        self::assertFalse($this->testHandler->hasWarningThatContains(self::DIFFERENT_LAYOUT_WARNING));
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testNewTypedTableGetsRangePartitioning(): void
    {
        $this->writeHeadlessCsv();

        $this->runOutputMapping(
            [
                'source' => self::SOURCE,
                'destination' => $this->getTableId(),
                'schema' => $this->getSchema(),
                'range_partitioning' => [
                    'field' => 'id',
                    'range' => ['start' => 0, 'end' => 100, 'interval' => 10],
                ],
            ],
            OutputMappingSettings::DATA_TYPES_SUPPORT_AUTHORITATIVE,
        );

        $definition = $this->getTableDefinition();
        self::assertSame('id', $definition['rangePartitioning']['field']);
        self::assertSame('0', (string) $definition['rangePartitioning']['range']['start']);
        self::assertSame('100', (string) $definition['rangePartitioning']['range']['end']);
        self::assertSame('10', (string) $definition['rangePartitioning']['range']['interval']);
        self::assertSame(2, $this->getRowsCount());
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testUntypedTableFromColumnsGetsClustering(): void
    {
        $this->writeHeadlessCsv();
        $this->writeColumnsManifest();

        $this->runOutputMapping(
            [
                'source' => self::SOURCE,
                'destination' => $this->getTableId(),
                'clustering' => ['fields' => ['country']],
            ],
            OutputMappingSettings::DATA_TYPES_SUPPORT_NONE,
        );

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->getTableId());
        self::assertFalse($table['isTyped']);
        self::assertSame(['country'], $table['definition']['clustering']['fields']);
        self::assertSame(2, $this->getRowsCount());
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testExistingTableWithDifferentLayoutLoadsAndLogsWarning(): void
    {
        $this->createTypedTableClusteredByCountry();
        $this->writeHeadlessCsv();

        $jobIds = $this->runOutputMapping(
            [
                'source' => self::SOURCE,
                'destination' => $this->getTableId(),
                'schema' => $this->getSchema(),
                'incremental' => false,
                'clustering' => ['fields' => ['id']],
            ],
            OutputMappingSettings::DATA_TYPES_SUPPORT_AUTHORITATIVE,
        );

        self::assertCount(1, $jobIds);
        $job = $this->clientWrapper->getBranchClient()->getJob($jobIds[0]);
        self::assertSame('success', $job['status']);
        self::assertSame(2, $this->getRowsCount());
        self::assertSame(['country'], $this->getTableDefinition()['clustering']['fields']);
        self::assertTrue($this->testHandler->hasWarningThatContains(self::DIFFERENT_LAYOUT_WARNING));
        self::assertTrue($this->testHandler->hasWarningThatContains('differs in "clustering"'));
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testExistingTableWithSameLayoutLogsNoWarning(): void
    {
        $this->createTypedTableClusteredByCountry();
        $this->writeHeadlessCsv();

        $jobIds = $this->runOutputMapping(
            [
                'source' => self::SOURCE,
                'destination' => $this->getTableId(),
                'schema' => $this->getSchema(),
                'incremental' => false,
                'clustering' => ['fields' => ['country']],
            ],
            OutputMappingSettings::DATA_TYPES_SUPPORT_AUTHORITATIVE,
        );

        self::assertCount(1, $jobIds);
        self::assertSame(2, $this->getRowsCount());
        self::assertSame(['country'], $this->getTableDefinition()['clustering']['fields']);
        self::assertFalse($this->testHandler->hasWarningThatContains(self::DIFFERENT_LAYOUT_WARNING));
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testLayoutWithoutColumnsFailsWithUserError(): void
    {
        file_put_contents(
            $this->temp->getTmpFolder() . '/upload/' . self::SOURCE,
            "\"id\",\"report_date\",\"country\"\n\"1\",\"2026-01-01\",\"cz\"\n",
        );

        try {
            $this->runOutputMapping(
                [
                    'source' => self::SOURCE,
                    'destination' => $this->getTableId(),
                    'clustering' => ['fields' => ['country']],
                ],
                OutputMappingSettings::DATA_TYPES_SUPPORT_NONE,
            );
            self::fail('Output mapping should fail with InvalidOutputException.');
        } catch (InvalidOutputException $e) {
            self::assertSame(
                sprintf(
                    'Cannot create table "%s" with the option(s) "clustering": the columns of the table are ' .
                    'unknown. Set "columns" or "schema" in the output mapping or in the manifest.',
                    $this->getTableId(),
                ),
                $e->getMessage(),
            );
        }

        self::assertFalse($this->clientWrapper->getTableAndFileStorageClient()->tableExists($this->getTableId()));
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testManifestLayoutCreatesTable(): void
    {
        $this->writeHeadlessCsv();
        $this->writeManifest([
            'schema' => $this->getSchema(),
            'time_partitioning' => ['type' => 'DAY', 'field' => 'report_date'],
            'clustering' => ['fields' => ['country']],
        ]);

        $this->runOutputMapping(
            [
                'source' => self::SOURCE,
                'destination' => $this->getTableId(),
            ],
            OutputMappingSettings::DATA_TYPES_SUPPORT_AUTHORITATIVE,
        );

        $this->assertTimePartitioningAndClustering();
        self::assertSame(2, $this->getRowsCount());
    }

    #[NeedsEmptyBigqueryOutputBucket]
    public function testConfigurationOverridesManifestLayout(): void
    {
        $this->writeHeadlessCsv();
        $this->writeManifest([
            'schema' => $this->getSchema(),
            'clustering' => ['fields' => ['country']],
        ]);

        $this->runOutputMapping(
            [
                'source' => self::SOURCE,
                'destination' => $this->getTableId(),
                'clustering' => ['fields' => ['id']],
            ],
            OutputMappingSettings::DATA_TYPES_SUPPORT_AUTHORITATIVE,
        );

        self::assertSame(['id'], $this->getTableDefinition()['clustering']['fields']);
        self::assertSame(2, $this->getRowsCount());
    }

    /**
     * @return string[] ids of the finished load jobs
     */
    private function runOutputMapping(array $config, string $dataTypeSupport): array
    {
        $tableQueue = $this->getTableLoader(logger: $this->testLogger)->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: ['mapping' => [$config]],
                sourcePathPrefix: 'upload',
                storageApiToken: $this->clientWrapper->getToken(),
                isFailedJob: false,
                dataTypeSupport: $dataTypeSupport,
            ),
            systemMetadata: new SystemMetadata(['componentId' => 'foo']),
        );

        return $tableQueue->waitForAll();
    }

    private function getTableId(): string
    {
        return $this->emptyBigqueryOutputBucketId . '.' . self::TABLE_NAME;
    }

    private function getTableDefinition(): array
    {
        return $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->getTableId())['definition'];
    }

    private function getRowsCount(): int
    {
        $preview = $this->clientWrapper->getTableAndFileStorageClient()->getTableDataPreview($this->getTableId());

        // the first line is the header
        return count(explode("\n", trim($preview))) - 1;
    }

    private function assertTimePartitioningAndClustering(): void
    {
        $definition = $this->getTableDefinition();
        self::assertSame('DAY', $definition['timePartitioning']['type']);
        self::assertSame('report_date', $definition['timePartitioning']['field']);
        self::assertSame(['country'], $definition['clustering']['fields']);
    }

    private function writeHeadlessCsv(): void
    {
        file_put_contents(
            $this->temp->getTmpFolder() . '/upload/' . self::SOURCE,
            "\"1\",\"2026-01-01\",\"cz\"\n\"2\",\"2026-01-02\",\"sk\"\n",
        );
    }

    private function writeManifest(array $manifest): void
    {
        file_put_contents(
            $this->temp->getTmpFolder() . '/upload/' . self::SOURCE . '.manifest',
            (string) json_encode($manifest),
        );
    }

    // with the slicing feature, `columns` are accepted from the manifest only
    private function writeColumnsManifest(): void
    {
        $this->writeManifest(['columns' => ['id', 'report_date', 'country']]);
    }

    // same payload output mapping sends for this schema, so the existing table matches the mapping
    private function createTypedTableClusteredByCountry(): void
    {
        $tableDefinition = new TableDefinitionFromSchema(
            self::TABLE_NAME,
            array_map(
                fn(array $column) => new MappingFromConfigurationSchemaColumn($column),
                $this->getSchema(),
            ),
            'bigquery',
        );
        $tableDefinition->setLayout(new MappingTableLayout(null, null, ['fields' => ['country']]));

        $this->clientWrapper->getTableAndFileStorageClient()->createTableDefinition(
            $this->emptyBigqueryOutputBucketId,
            $tableDefinition->getRequestData(),
        );
    }

    private function getSchema(): array
    {
        return [
            [
                'name' => 'id',
                'data_type' => [
                    'base' => ['type' => BaseType::INTEGER],
                    'bigquery' => ['type' => Bigquery::TYPE_INT64],
                ],
                'nullable' => false,
                'primary_key' => true,
            ],
            [
                'name' => 'report_date',
                'data_type' => [
                    'base' => ['type' => BaseType::DATE],
                    'bigquery' => ['type' => Bigquery::TYPE_DATE],
                ],
            ],
            [
                'name' => 'country',
                'data_type' => [
                    'base' => ['type' => BaseType::STRING],
                    'bigquery' => ['type' => Bigquery::TYPE_STRING],
                ],
            ],
        ];
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
