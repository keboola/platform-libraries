<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\TableExportQueue;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApiBranch\ClientWrapper;
use Monolog\Handler\TestHandler;
use Monolog\Level;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class LocalStrategyLoggingTest extends TestCase
{
    private TestHandler $testHandler;
    private Logger $testLogger;
    private string $tmpDir;

    public function setUp(): void
    {
        parent::setUp();

        $this->testHandler = new TestHandler();
        $this->testLogger = new Logger('testLogger', [$this->testHandler]);
        $this->tmpDir = sys_get_temp_dir() . '/' . uniqid('im-local-log-', true);
        mkdir($this->tmpDir, 0777, true);
    }

    public function tearDown(): void
    {
        array_map('unlink', glob($this->tmpDir . '/destination/*.manifest') ?: []);
        @rmdir($this->tmpDir . '/destination');
        @rmdir($this->tmpDir);

        parent::tearDown();
    }

    public function testEachTableIsDownloadedAndLoggedSeparately(): void
    {
        $jobResults = [
            [
                'id' => 100,
                'status' => 'success',
                'tableId' => 'in.c-test-bucket.table1',
                'metrics' => ['outBytes' => 1048576, 'outBytesUncompressed' => 2097152],
                'results' => ['file' => ['id' => 555]],
            ],
            [
                'id' => 101,
                'status' => 'success',
                'tableId' => 'in.c-test-bucket.table2',
                'metrics' => ['outBytes' => 2097152, 'outBytesUncompressed' => 4194304],
                'results' => ['file' => ['id' => 556]],
            ],
        ];

        $storageClient = $this->createMock(Client::class);
        $storageClient->expects($this->once())
            ->method('handleAsyncTasks')
            ->with([100, 101])
            ->willReturn($jobResults);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getTableAndFileStorageClient')
            ->willReturn($storageClient);

        $expectedJobIds = [100, 101];
        $tableExporter = $this->createMock(TableExporter::class);
        $tableExporter->expects($this->exactly(2))
            ->method('downloadExportedFiles')
            ->willReturnCallback(
                function (array $downloadedResults, array $exportJobs) use (&$expectedJobIds): void {
                    // one call per table, in queue order - this is what makes per-table timing possible
                    self::assertCount(1, $downloadedResults);
                    self::assertSame(array_shift($expectedJobIds), $downloadedResults[0]['id']);
                    self::assertArrayHasKey(100, $exportJobs);
                    self::assertArrayHasKey(101, $exportJobs);
                },
            );

        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $metadataStorage->expects($this->exactly(2))
            ->method('getPath')
            ->willReturn($this->tmpDir);

        $strategy = new TestLocalStrategy(
            $clientWrapper,
            $this->testLogger,
            $this->createMock(FileStagingInterface::class),
            $metadataStorage,
            new InputTableStateList([]),
            'destination',
            FileFormat::Json,
        );
        $strategy->setTableExporter($tableExporter);

        $result = $strategy->waitForTableLoadCompletion($this->createQueue());

        self::assertCount(2, $result->getTables());

        $messages = $this->messagesAtLevel(Level::Info);

        self::assertContains('Waiting for 2 storage jobs to finish.', $messages);
        self::assertContains('Downloading 2 exported tables.', $messages);
        self::assertContains('Fetching table in.c-test-bucket.table1 (export job 100, file 555).', $messages);
        self::assertContains('Fetching table in.c-test-bucket.table2 (export job 101, file 556).', $messages);

        $firstFetched = self::indexOfMessageContaining($messages, 'Fetched table in.c-test-bucket.table1.');
        $secondFetching = self::indexOfMessageContaining($messages, 'Fetching table in.c-test-bucket.table2');

        // the regression guard for AJDA-3148: per-table lines are interleaved with the downloads,
        // they are not all emitted after the whole batch has finished
        self::assertLessThan($secondFetching, $firstFetched);

        self::assertStringStartsWith(
            'Fetched table in.c-test-bucket.table1. Downloaded 1048576 bytes in ',
            $messages[$firstFetched],
        );
        self::assertStringEndsWith('export job 100, file 555.', $messages[$firstFetched]);

        self::assertStringStartsWith(
            'Downloaded 2 tables, 3145728 bytes in ',
            $messages[self::indexOfMessageContaining($messages, 'Downloaded 2 tables,')],
        );
        self::assertStringStartsWith(
            'Wrote 2 table manifests in ',
            $messages[self::indexOfMessageContaining($messages, 'Wrote 2 table manifests')],
        );
        self::assertStringStartsWith(
            'All tables were fetched. 2 tables in ',
            $messages[self::indexOfMessageContaining($messages, 'All tables were fetched.')],
        );
    }

    /**
     * @return string[]
     */
    private function messagesAtLevel(Level $level): array
    {
        $messages = [];
        foreach ($this->testHandler->getRecords() as $record) {
            if ($record->level === $level) {
                $messages[] = $record->message;
            }
        }

        return $messages;
    }

    private function createQueue(): TableExportQueue
    {
        return new TableExportQueue(
            [
                100 => $this->createTableOptions('in.c-test-bucket.table1', 'table1'),
                101 => $this->createTableOptions('in.c-test-bucket.table2', 'table2'),
            ],
            TestLocalStrategy::class,
            'destination',
            [
                100 => [
                    'tableId' => 'in.c-test-bucket.table1',
                    'destination' => $this->tmpDir . '/destination/table1',
                    'exportOptions' => [],
                ],
                101 => [
                    'tableId' => 'in.c-test-bucket.table2',
                    'destination' => $this->tmpDir . '/destination/table2',
                    'exportOptions' => [],
                ],
            ],
        );
    }

    private function createTableOptions(string $tableId, string $tableName): RewrittenInputTableOptions
    {
        return new RewrittenInputTableOptions(
            ['source' => $tableId, 'destination' => $tableName],
            $tableId,
            123,
            [
                'id' => $tableId,
                'uri' => 'https://connection.keboola.com/v2/storage/tables/' . $tableId,
                'name' => $tableName,
                'displayName' => $tableName,
                'primaryKey' => [],
                'distributionKey' => [],
                'created' => '2022-06-03T01:31:43+0200',
                'lastChangeDate' => '2022-06-03T02:31:43+0200',
                'lastImportDate' => '2022-06-03T03:31:43+0200',
                'columns' => ['col1'],
                'metadata' => [],
                'columnMetadata' => [],
            ],
        );
    }

    /**
     * @param string[] $messages
     */
    private static function indexOfMessageContaining(array $messages, string $needle): int
    {
        foreach ($messages as $index => $message) {
            if (str_contains($message, $needle)) {
                return $index;
            }
        }

        self::fail(sprintf('No log message contains "%s".', $needle));
    }
}
