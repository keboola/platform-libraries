<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests;

use Generator;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Tests\Needs\TestSatisfyer;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use PHPUnit\Util\Test;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ReflectionObject;
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
    protected string $emptyRedshiftOutputBucketId;
    protected string $emptyRedshiftInputBucketId;
    protected string $testBucketId;
    protected string $firstTableId;
    protected string $secondTableId;
    protected string $thirdTableId;

    public function setUp(): void
    {
        parent::setUp();
        $this->temp = new Temp('output-mapping');
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload');
        $fs->mkdir($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');

        $this->initClient();
        $method = $this->getName(false);

        $objects = TestSatisfyer::satisfyTestNeeds(
            new ReflectionObject($this),
            $this->clientWrapper,
            $this->temp,
            $method,
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
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        foreach ($files as $file) {
            $this->clientWrapper->getBasicClient()->deleteFile($file['id']);
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
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $this->clientWrapper->getBasicClient()->getApiUrl()
        ));
    }

    public function tearDown(): void
    {
        if ($this->workspaceId) {
            $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
            try {
                $workspaces->deleteWorkspace($this->workspaceId, [], true);
            } catch (ClientException $e) {
                if ($e->getCode() !== 404) {
                    throw $e;
                }
            }
            $this->workspaceId = null;
        }
        parent::tearDown();
    }

    protected function getWorkspaceStagingFactory(
        ?ClientWrapper $clientWrapper = null,
        string $format = 'json',
        ?LoggerInterface $logger = null,
        array $backend = [AbstractStrategyFactory::WORKSPACE_SNOWFLAKE, 'snowflake']
    ): StrategyFactory {
        $stagingFactory = new StrategyFactory(
            $clientWrapper ?: $this->clientWrapper,
            $logger ?: new NullLogger(),
            $format
        );
        $mockWorkspace = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getWorkspaceId', 'getCredentials'])
            ->getMock();
        $mockWorkspace->method('getWorkspaceId')->willReturnCallback(
            function () use ($backend) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->clientWrapper->getBranchClientIfAvailable());
                    $workspace = $workspaces->createWorkspace(['backend' => $backend[1]], true);
                    $this->workspaceId = (string) $workspace['id'];
                    $this->workspaceCredentials = $workspace['connection'];
                }
                return $this->workspaceId;
            }
        );
        $mockWorkspace->method('getCredentials')->willReturnCallback(
            function () use ($backend) {
                if (!$this->workspaceCredentials) {
                    $workspaces = new Workspaces($this->clientWrapper->getBranchClientIfAvailable());
                    $workspace = $workspaces->createWorkspace(['backend' => $backend[1]], true);
                    $this->workspaceId = (string) $workspace['id'];
                    $this->workspaceCredentials = $workspace['connection'];
                }
                return $this->workspaceCredentials;
            }
        );
        $mockLocal = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getPath'])
            ->getMock();
        $mockLocal->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            }
        );
        /** @var ProviderInterface $mockLocal */
        /** @var ProviderInterface $mockWorkspace */
        $stagingFactory->addProvider(
            $mockLocal,
            [
                $backend[0] => new Scope([Scope::FILE_METADATA, Scope::TABLE_METADATA]),
            ]
        );
        $stagingFactory->addProvider(
            $mockWorkspace,
            [
                $backend[0] => new Scope([Scope::FILE_DATA, Scope::TABLE_DATA]),
            ]
        );

        $mockLocal = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getPath'])
            ->getMock();
        $mockLocal->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            }
        );
        /** @var ProviderInterface $mockLocal */
        $stagingFactory->addProvider(
            $mockLocal,
            [
                AbstractStrategyFactory::LOCAL => new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA]),
            ]
        );
        $stagingFactory->addProvider(
            $mockLocal,
            [
                AbstractStrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA]),
            ]
        );
        return $stagingFactory;
    }

    protected function getLocalStagingFactory(
        ?ClientWrapper $clientWrapper = null,
        string $format = 'json',
        ?LoggerInterface $logger = null
    ): StrategyFactory {
        $stagingFactory = new StrategyFactory(
            $clientWrapper ?: $this->clientWrapper,
            $logger ?: new NullLogger(),
            $format
        );
        $mockLocal = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getPath'])
            ->getMock();
        $mockLocal->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            }
        );
        /** @var ProviderInterface $mockLocal */
        $stagingFactory->addProvider(
            $mockLocal,
            [
                AbstractStrategyFactory::LOCAL => new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA]),
            ]
        );
        $stagingFactory->addProvider(
            $mockLocal,
            [
                AbstractStrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA]),
            ]
        );
        return $stagingFactory;
    }

    protected function assertTablesExists(string $bucketId, array $expectedTables): void
    {
        $tables = $this->clientWrapper->getBasicClient()->listTables($bucketId);
        $tableIds = array_column($tables, 'id');

        // order of listed tables is not guaranteed
        sort($tableIds);
        sort($expectedTables);

        self::assertSame($expectedTables, $tableIds);
    }

    protected function assertTableRowsEquals(string $tableName, array $expectedRows): void
    {
        $data = $this->clientWrapper->getBasicClient()->getTableDataPreview($tableName);

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
        $job = $this->clientWrapper->getBasicClient()->getJob($jobId);
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
        string $expectedColumnName
    ): void {
        self::assertSame('tableColumnAdd', $jobData['operationName']);
        self::assertSame('success', $jobData['status']);
        self::assertSame($expectedColumnName, $jobData['operationParams']['name']);
        self::assertArrayNotHasKey('basetype', $jobData['operationParams']);
        self::assertArrayNotHasKey('definition', $jobData['operationParams']);
    }

    public function incrementalFlagProvider(): Generator
    {
        yield 'incremental load' => [true];
        yield 'full load' => [false];
    }

    protected function prepareWorkspaceWithTables(string $inputBucketId, string $tablePrefix = ''): void
    {
        $workspaces = new Workspaces($this->clientWrapper->getBranchClientIfAvailable());
        $workspaces->loadWorkspaceData(
            $this->workspaceId,
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
            ]
        );
    }

    protected function prepareWorkspaceWithTablesClone(string $inputBucketId, string $tablePrefix = ''): void
    {
        $workspaces = new Workspaces($this->clientWrapper->getBranchClientIfAvailable());
        $workspaces->cloneIntoWorkspace(
            $this->workspaceId,
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
            ]
        );
    }
}
