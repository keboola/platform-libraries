<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\TableLayout;

use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Tests\Needs\NeedsEmptyOutputBucket;

class TableLayoutSnowflakeTest extends AbstractTestCase
{
    private const SOURCE = 'tableLayout.csv';
    private const OTHER_BACKEND_WARNING = '": the option(s) "time_partitioning", "clustering" have no effect on the ' .
        '"snowflake" backend and were not sent to Storage.';
    private const DIFFERENT_LAYOUT_WARNING = 'already exists with layout';

    #[NeedsEmptyOutputBucket]
    public function testLayoutKeysAreNotSentOnSnowflake(): void
    {
        $this->runOutputMapping();

        $table = $this->clientWrapper->getTableAndFileStorageClient()->getTable($this->getTableId());
        self::assertSame(2, (int) $table['rowsCount']);
        $definition = $table['definition'] ?? [];
        self::assertArrayNotHasKey('timePartitioning', $definition);
        self::assertArrayNotHasKey('rangePartitioning', $definition);
        self::assertArrayNotHasKey('clustering', $definition);
        self::assertTrue($this->testHandler->hasWarningThatContains(self::OTHER_BACKEND_WARNING));
    }

    #[NeedsEmptyOutputBucket]
    public function testLayoutKeysOnExistingSnowflakeTableLogWarning(): void
    {
        $this->runOutputMapping();
        $this->testHandler->clear();

        $this->runOutputMapping();

        self::assertTrue($this->testHandler->hasWarningThatContains(self::OTHER_BACKEND_WARNING));
        self::assertFalse($this->testHandler->hasWarningThatContains(self::DIFFERENT_LAYOUT_WARNING));
    }

    private function runOutputMapping(): void
    {
        file_put_contents(
            $this->temp->getTmpFolder() . '/upload/' . self::SOURCE,
            "\"1\",\"2026-01-01\",\"cz\"\n\"2\",\"2026-01-02\",\"sk\"\n",
        );

        $tableQueue = $this->getTableLoader(logger: $this->testLogger)->uploadTables(
            configuration: new OutputMappingSettings(
                configuration: [
                    'mapping' => [
                        [
                            'source' => self::SOURCE,
                            'destination' => $this->getTableId(),
                            'columns' => ['id', 'report_date', 'country'],
                            'time_partitioning' => ['type' => 'DAY', 'field' => 'report_date'],
                            'clustering' => ['fields' => ['country']],
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
        self::assertCount(1, $tableQueue->waitForAll());
    }

    private function getTableId(): string
    {
        return $this->emptyOutputBucketId . '.tableLayout';
    }
}
