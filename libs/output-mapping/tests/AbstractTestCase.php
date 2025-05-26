<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests;

use InvalidArgumentException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\TableLoader;
use Keboola\OutputMapping\Tests\Needs\TestSatisfyer;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use PHPUnit\Util\Test;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ReflectionObject;
use RuntimeException;
use Symfony\Component\Filesystem\Filesystem;

abstract class AbstractTestCase extends TestCase
{
    protected ClientWrapper $clientWrapper;
    protected Temp $temp;

    protected ?string $workspaceId = null;
    protected array $workspaceCredentials = [];
    protected array $workspace;

    protected string $emptyInputBucketId;
    protected string $emptyOutputBucketId;
    protected string $emptyBigqueryOutputBucketId;
    protected string $testBucketId;
    protected string $firstTableId;
    protected string $secondTableId;
    protected string $thirdTableId;

    protected string $devBranchName;
    protected string $devBranchId;

    protected TestHandler $testHandler;
    protected Logger $testLogger;

    public function setUp(): void
    {
        parent::setUp();

        $this->testHandler = new TestHandler();
        $this->testLogger = new Logger('testLogger', [$this->testHandler]);

        $this->temp = $this->createTemp();

        $this->initClient();
        $method = $this->getName(false);

        $objects = TestSatisfyer::satisfyTestNeeds(
            new ReflectionObject($this),
            $this->clientWrapper,
            $this->temp,
            $method,
            (string) $this->dataName(),
        );
        foreach ($objects as $name => $value) {
            if ($value !== null) {
                $this->$name = $value;
            }
        }
    }

    protected function clearFileUploads(array $tags): void
    {
        // Delete file uploads
        $options = new ListFilesOptions();
        $options->setTags($tags);
        sleep(1);
        $files = $this->clientWrapper->getTableAndFileStorageClient()->listFiles($options);
        foreach ($files as $file) {
            $this->clientWrapper->getTableAndFileStorageClient()->deleteFile($file['id']);
        }
    }

    protected function initClient(?string $branchId = null): void
    {
        $clientOptions = (new ClientOptions())
            ->setUrl((string) getenv('STORAGE_API_URL'))
            ->setToken((string) getenv('STORAGE_API_TOKEN'))
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

    public function tearDown(): void
    {
        if ($this->workspaceId) {
            $workspaces = new Workspaces($this->clientWrapper->getBranchClient());
            try {
                $workspaces->deleteWorkspace((int) $this->workspaceId, [], true);
            } catch (ClientException $e) {
                if ($e->getCode() !== 404) {
                    throw $e;
                }
            }
            $this->workspaceId = null;
        }
        parent::tearDown();
    }

    protected function initWorkspace(): void
    {
        $workspaces = new Workspaces($this->clientWrapper->getBranchClient());

        if ($this->workspaceId) {
            $workspaces->deleteWorkspace((int) $this->workspaceId, async: true);
        }

        $stagingType = StagingType::from('workspace-' . $this->clientWrapper->getToken()->getProjectBackend());

        $workspace = $workspaces->createWorkspace(['backend' => match ($stagingType) {
            StagingType::WorkspaceSnowflake => 'snowflake',
            StagingType::WorkspaceBigquery => 'bigquery',
            default => throw new InvalidArgumentException(sprintf(
                'Unknown staging %s',
                $stagingType->value,
            )),
        }], true);

        $this->workspaceId = (string) $workspace['id'];
        $this->workspaceCredentials = $workspace['connection'];
    }

    protected function getWorkspaceStagingFactory(
        ?ClientWrapper $clientWrapper = null,
        FileFormat $format = FileFormat::Json,
        ?LoggerInterface $logger = null,
        ?string $workspaceId = null,
    ): StrategyFactory {
        $clientWrapper ??= $this->clientWrapper;

        $stagingType = StagingType::from('workspace-' . $clientWrapper->getToken()->getProjectBackend());

        $workspaceId ??= $this->workspaceId ??
            throw new RuntimeException('Run initWorkspace before getWorkspaceStagingFactory');

        return new StrategyFactory(
            new StagingProvider(
                stagingType: $stagingType,
                localStagingPath: $this->temp->getTmpFolder(),
                stagingWorkspaceId: $workspaceId,
            ),
            $clientWrapper,
            $logger ?: new NullLogger(),
            $format,
        );
    }

    protected function getLocalStagingFactory(
        ?ClientWrapper $clientWrapper = null,
        FileFormat $format = FileFormat::Json,
        ?LoggerInterface $logger = null,
        ?string $stagingPath = null,
    ): StrategyFactory {
        $clientWrapper ??= $this->clientWrapper;

        $fileStaging = $this->createMock(FileStagingInterface::class);
        $fileStaging->method('getPath')->willReturnCallback(
            fn() => $stagingPath ?? $this->temp->getTmpFolder(),
        );

        return new StrategyFactory(
            new StagingProvider(
                stagingType: StagingType::Local,
                localStagingPath: $stagingPath ?? $this->temp->getTmpFolder(),
                stagingWorkspaceId: null,
            ),
            $clientWrapper,
            $logger ?: new NullLogger(),
            $format,
        );
    }

    public function getTableLoader(
        ?ClientWrapper $clientWrapper = null,
        ?LoggerInterface $logger = null,
        ?StrategyFactory $strategyFactory = null,
    ): TableLoader {
        $clientWrapper ??= $this->clientWrapper;
        $logger ??= new NullLogger();
        $strategyFactory ??= $this->getLocalStagingFactory(
            clientWrapper: $clientWrapper,
            logger: $logger,
        );

        return new TableLoader(
            $logger,
            $clientWrapper,
            $strategyFactory,
        );
    }

    protected function assertTablesExists(string $bucketId, array $expectedTables): void
    {
        $tables = $this->clientWrapper->getTableAndFileStorageClient()->listTables($bucketId);
        $tableIds = array_column($tables, 'id');

        // order of listed tables is not guaranteed
        sort($tableIds);
        sort($expectedTables);

        self::assertSame($expectedTables, $tableIds);
    }

    protected function assertTableRowsEquals(string $tableName, array $expectedRows): void
    {
        $data = $this->clientWrapper->getTableAndFileStorageClient()->getTableDataPreview($tableName);

        $rows = explode("\n", trim($data));
        // convert to lowercase because of https://keboola.atlassian.net/browse/KBC-864
        $rows = array_map('strtolower', $rows);

        // order of rows is not guaranteed
        sort($rows);
        sort($expectedRows);

        // Both id and name columns are present because of https://keboola.atlassian.net/browse/KBC-865
        self::assertEquals($expectedRows, $rows);
    }

    protected function assertJobParamsMatches(array $expectedParams, string $jobId): void
    {
        /** @var array{
         *     operationParams: array{params: array},
         * } $job
         */
        $job = $this->clientWrapper->getBranchClient()->getJob($jobId);
        foreach ($expectedParams as $expectedParam) {
            self::assertContains($expectedParam, $job['operationParams']['params']);
        }
    }

    protected static function assertTableImportJob(array $jobData, bool $expectedIncrementalFlag): void
    {
        self::assertSame('tableImport', $jobData['operationName']);
        self::assertSame('success', $jobData['status']);
        self::assertSame($expectedIncrementalFlag, $jobData['operationParams']['params']['incremental']);
        self::assertSame([], $jobData['results']['newColumns']);
    }

    protected static function assertTableColumnAddJob(
        array $jobData,
        string $expectedColumnName,
    ): void {
        self::assertSame('tableColumnAdd', $jobData['operationName']);
        self::assertSame('success', $jobData['status']);
        self::assertSame($expectedColumnName, $jobData['operationParams']['name']);
        self::assertArrayNotHasKey('basetype', $jobData['operationParams']);
        self::assertArrayNotHasKey('definition', $jobData['operationParams']);
    }

    public function incrementalFlagProvider(): iterable
    {
        yield 'incremental load' => [true];
        yield 'full load' => [false];
    }

    protected function prepareWorkspaceWithTables(string $inputBucketId, string $tablePrefix = ''): void
    {
        $workspaces = new Workspaces($this->clientWrapper->getBranchClient());
        $workspaces->loadWorkspaceData(
            (int) $this->workspaceId,
            [
                'input' => [
                    [
                        'source' => $inputBucketId . '.test1',
                        'destination' => $tablePrefix . 'table1a',
                    ],
                    [
                        'source' => $inputBucketId . '.test2',
                        'destination' => $tablePrefix . 'table2a',
                    ],
                ],
            ],
        );
    }

    protected function prepareWorkspaceWithTablesClone(string $inputBucketId, string $tablePrefix = ''): void
    {
        $workspaces = new Workspaces($this->clientWrapper->getBranchClient());
        $workspaces->cloneIntoWorkspace(
            (int) $this->workspaceId,
            [
                'input' => [
                    [
                        'source' => $inputBucketId . '.test1',
                        'destination' => $tablePrefix . 'table1a',
                    ],
                    [
                        'source' => $inputBucketId . '.test2',
                        'destination' => $tablePrefix . 'table2a',
                    ],
                ],
            ],
        );
    }

    protected function createTemp(string $prefix = 'output-mapping'): Temp
    {
        $temp = new Temp($prefix);
        $fs = new Filesystem();
        $fs->mkdir($temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload');
        $fs->mkdir($temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        return $temp;
    }

    public static function assertLinesEqualsSorted(string $expected, string $actual): void
    {
        $expected = explode("\n", $expected);
        $actual = explode("\n", $actual);
        sort($expected);
        sort($actual);
        self::assertSame($expected, $actual);
    }
}
