<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests;

use InvalidArgumentException;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\Tests\Needs\TestSatisfyer;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Staging\Workspace\NullWorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApi\Client;
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
use Symfony\Component\Filesystem\Filesystem;

abstract class AbstractTestCase extends TestCase
{
    /** @deprecated use initClient() instead */
    protected ClientWrapper $clientWrapper;
    protected Temp $temp;
    protected TestHandler $testHandler;
    protected Logger $testLogger;
    protected ?string $workspaceId = null;
    protected ?Workspaces $workspaceClient = null;
    protected array $workspaceCredentials;

    protected string $emptyInputBucketId;
    protected string $emptyOutputBucketId;
    protected string $testBucketId;
    protected string $firstTableId;
    protected string $secondTableId;
    protected string $thirdTableId;

    protected string $devBranchName;
    protected string $devBranchId;

    protected string $emptyBranchInputBucketId;

    public function setUp(): void
    {
        parent::setUp();

        $this->testHandler = new TestHandler();
        $this->testLogger = new Logger('testLogger', [$this->testHandler]);

        $this->temp = new Temp('input-mapping');
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/download');
        $this->clientWrapper = $this->initClient();

        $objects = TestSatisfyer::satisfyTestNeeds(
            new ReflectionObject($this),
            $this->initClient(),
            $this->temp,
            $this->getName(false),
            (string) $this->dataName(),
        );
        foreach ($objects as $name => $value) {
            if ($value !== null) {
                $this->$name = $value;
            }
        }
    }

    protected function initClient(?string $branchId = null): ClientWrapper
    {
        $clientOptions = (new ClientOptions())
            ->setUrl((string) getenv('STORAGE_API_URL'))
            ->setToken((string) getenv('STORAGE_API_TOKEN'))
            ->setBranchId($branchId)
            ->setBackoffMaxTries(1)
            ->setJobPollRetryDelay(function () {
                return 1;
            })
            ->setUserAgent(implode('::', Test::describe($this)))
        ;

        $clientWrapper = new ClientWrapper($clientOptions);
        $tokenInfo = $clientWrapper->getBranchClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $clientWrapper->getBranchClient()->getApiUrl(),
        ));
        return $clientWrapper;
    }

    public function tearDown(): void
    {
        if ($this->workspaceId && $this->workspaceClient) {
            $this->workspaceClient->deleteWorkspace((int) $this->workspaceId, [], true);
            $this->workspaceId = null;
        }
        parent::tearDown();
    }

    protected function assertS3info(array $manifest): void
    {
        self::assertArrayHasKey('s3', $manifest);
        self::assertArrayHasKey('isSliced', $manifest['s3']);
        self::assertArrayHasKey('region', $manifest['s3']);
        self::assertArrayHasKey('bucket', $manifest['s3']);
        self::assertArrayHasKey('key', $manifest['s3']);
        self::assertArrayHasKey('credentials', $manifest['s3']);
        self::assertArrayHasKey('access_key_id', $manifest['s3']['credentials']);
        self::assertArrayHasKey('secret_access_key', $manifest['s3']['credentials']);
        self::assertArrayHasKey('session_token', $manifest['s3']['credentials']);
        self::assertStringContainsString('gz', $manifest['s3']['key']);

        if ($manifest['s3']['isSliced']) {
            self::assertStringContainsString('manifest', $manifest['s3']['key']);
        }
    }

    protected function assertABSinfo(array $manifest): void
    {
        self::assertArrayHasKey('abs', $manifest);
        self::assertArrayHasKey('is_sliced', $manifest['abs']);
        self::assertArrayHasKey('region', $manifest['abs']);
        self::assertArrayHasKey('container', $manifest['abs']);
        self::assertArrayHasKey('name', $manifest['abs']);
        self::assertArrayHasKey('credentials', $manifest['abs']);
        self::assertArrayHasKey('sas_connection_string', $manifest['abs']['credentials']);
        self::assertArrayHasKey('expiration', $manifest['abs']['credentials']);

        if ($manifest['abs']['is_sliced']) {
            self::assertStringEndsWith('manifest', $manifest['abs']['name']);
        }
    }

    public static function assertCSVEquals(string $expectedString, string $path): void
    {
        $expectedArray = explode("\n", $expectedString);
        $actualArray = explode("\n", (string) file_get_contents($path));

        // compare length
        self::assertEquals(count($expectedArray), count($actualArray));
        // compare headers
        self::assertEquals($expectedArray[0], $actualArray[0]);

        $actualArrayWithoutHeader = array_slice($actualArray, 1);
        // compare each line
        for ($i = 1; $i < count($expectedArray); $i++) {
            self::assertTrue(in_array($expectedArray[$i], $actualArrayWithoutHeader));
        }
    }

    protected function getWorkspaceStagingFactory(
        ClientWrapper $clientWrapper,
        string $format = 'json',
        ?LoggerInterface $logger = null,
        StagingType $stagingType = StagingType::WorkspaceSnowflake,
    ): StrategyFactory {
        $workspaceStaging = $this->createMock(WorkspaceStagingInterface::class);
        $workspaceStaging->method('getWorkspaceId')->willReturnCallback(
            function () use ($stagingType, $clientWrapper) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($clientWrapper->getBranchClient());
                    $workspace = $workspaces->createWorkspace(['backend' => match($stagingType) {
                        StagingType::WorkspaceSnowflake => 'snowflake',
                        StagingType::WorkspaceBigquery => 'bigquery',
                        default => throw new InvalidArgumentException(sprintf(
                            'Unknown staging %s',
                            $stagingType->value,
                        )),
                    }], true);
                    $this->workspaceId = (string) $workspace['id'];
                    $this->workspaceCredentials = $workspace['connection'];
                    $this->workspaceClient = $workspaces;
                }
                return $this->workspaceId;
            },
        );

        $fileStaging = $this->createMock(FileStagingInterface::class);
        $fileStaging->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            },
        );

        return new StrategyFactory(
            new StagingProvider(
                $stagingType,
                $workspaceStaging,
                $fileStaging,
            ),
            $clientWrapper,
            $logger ?: new NullLogger(),
            $format,
        );
    }

    protected function getLocalStagingFactory(
        ClientWrapper $clientWrapper,
        string $format = 'json',
        ?LoggerInterface $logger = null,
        StagingType $stagingType = StagingType::Local,
    ): StrategyFactory {
        $fileStaging = $this->createMock(FileStagingInterface::class);
        $fileStaging->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            },
        );

        return new StrategyFactory(
            new StagingProvider(
                $stagingType,
                new NullWorkspaceStaging(),
                $fileStaging,
            ),
            $clientWrapper,
            $logger ?: new NullLogger(),
            $format,
        );
    }

    protected function clearFileUploads(array $tags): void
    {
        $clientWrapper = $this->initClient();

        // Delete all file uploads with specified tags
        $options = new ListFilesOptions();
        $options->setTags($tags);

        while ($files = $clientWrapper->getTableAndFileStorageClient()->listFiles($options)) {
            foreach ($files as $file) {
                try {
                    $clientWrapper->getTableAndFileStorageClient()->deleteFile($file['id']);
                } catch (ClientException $e) {
                    if ($e->getCode() !== 404) {
                        throw $e;
                    }
                }
            }
        }
    }

    protected function initEmptyFakeBranchInputBucket(ClientWrapper $clientWrapper): void
    {
        $emptyInputBucket = $clientWrapper->getTableAndFileStorageClient()->getBucket($this->emptyInputBucketId);

        foreach ($clientWrapper->getTableAndFileStorageClient()->listBuckets() as $bucket) {
            if (preg_match('/^(c-)?[0-9]+-' . $emptyInputBucket['displayName'] . '$/ui', $bucket['name'])) {
                $clientWrapper->getTableAndFileStorageClient()->dropBucket(
                    $bucket['id'],
                    ['force' => true, 'async' => true],
                );
            }
        }

        $this->emptyBranchInputBucketId = $clientWrapper->getTableAndFileStorageClient()->createBucket(
            $this->devBranchId . '-' . $emptyInputBucket['displayName'],
            Client::STAGE_IN,
        );
    }

    protected function initEmptyRealBranchInputBucket(ClientWrapper $clientWrapper): void
    {
        $emptyInputBucket = $clientWrapper->getTableAndFileStorageClient()->getBucket($this->emptyInputBucketId);

        foreach ($clientWrapper->getTableAndFileStorageClient()->listBuckets() as $bucket) {
            if (preg_match('/^(c-)?[0-9]+-' . $emptyInputBucket['displayName'] . '$/ui', $bucket['name'])) {
                $clientWrapper->getTableAndFileStorageClient()->dropBucket(
                    $bucket['id'],
                    ['force' => true, 'async' => true],
                );
            }
        }

        $clientWraper = new ClientWrapper(
            $clientWrapper->getClientOptionsReadOnly()->setBranchId($this->devBranchId),
        );

        $this->emptyBranchInputBucketId = $clientWraper->getBranchClient()->createBucket(
            $emptyInputBucket['displayName'],
            Client::STAGE_IN,
        );
    }

    protected function getFileTag(string $suffix = ''): string
    {
        $tag =  (new ReflectionObject($this))->getShortName();
        $tag .= '_' . $this->getName(false);
        $dataName = (string) $this->dataName();

        if ($dataName) {
            $tag .= '_' . $dataName;
        }

        return $tag . $suffix;
    }
}
