<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests;

use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\Tests\Needs\TestSatisfyer;
use Keboola\StorageApi\Client;
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
    protected ClientWrapper $clientWrapper;
    protected Temp $temp;
    protected TestHandler $testHandler;
    protected Logger $testLogger;
    protected ?string $workspaceId = null;
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
        $this->initClient();

        $objects = TestSatisfyer::satisfyTestNeeds(
            new ReflectionObject($this),
            $this->clientWrapper,
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
            ->setUserAgent(implode('::', Test::describe($this)))
        ;

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
            $workspaces->deleteWorkspace((int) $this->workspaceId, [], true);
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
        ?ClientWrapper $clientWrapper = null,
        string $format = 'json',
        ?LoggerInterface $logger = null,
        array $backend = [AbstractStrategyFactory::WORKSPACE_SNOWFLAKE, 'snowflake'],
    ): StrategyFactory {
        $stagingFactory = new StrategyFactory(
            $clientWrapper ?: $this->clientWrapper,
            $logger ?: new NullLogger(),
            $format,
        );
        $mockWorkspace = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getWorkspaceId'])
            ->getMock();
        $mockWorkspace->method('getWorkspaceId')->willReturnCallback(
            function () use ($backend) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->clientWrapper->getBranchClient());
                    $workspace = $workspaces->createWorkspace(['backend' => $backend[1]], true);
                    $this->workspaceId = (string) $workspace['id'];
                    $this->workspaceCredentials = $workspace['connection'];
                }
                return $this->workspaceId;
            },
        );
        $mockLocal = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getPath'])
            ->getMock();
        $mockLocal->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            },
        );
        /** @var ProviderInterface $mockLocal */
        /** @var ProviderInterface $mockWorkspace */
        $stagingFactory->addProvider(
            $mockLocal,
            [
                $backend[0] => new Scope([Scope::TABLE_METADATA]),
            ],
        );
        $stagingFactory->addProvider(
            $mockWorkspace,
            [
                $backend[0] => new Scope([Scope::TABLE_DATA]),
            ],
        );
        return $stagingFactory;
    }

    protected function getLocalStagingFactory(
        ?ClientWrapper $clientWrapper = null,
        string $format = 'json',
        ?LoggerInterface $logger = null,
    ): StrategyFactory {
        $stagingFactory = new StrategyFactory(
            $clientWrapper ?: $this->clientWrapper,
            $logger ?: new NullLogger(),
            $format,
        );
        $mockLocal = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getPath'])
            ->getMock();
        $mockLocal->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            },
        );
        /** @var ProviderInterface $mockLocal */
        $stagingFactory->addProvider(
            $mockLocal,
            [
                AbstractStrategyFactory::LOCAL => new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA]),
                AbstractStrategyFactory::ABS => new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA]),
                AbstractStrategyFactory::S3 => new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA]),
            ],
        );
        $stagingFactory->addProvider(
            $mockLocal,
            [
                AbstractStrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA]),
            ],
        );
        return $stagingFactory;
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

    protected function initEmptyFakeBranchInputBucket(): void
    {
        $emptyInputBucket = $this->clientWrapper->getTableAndFileStorageClient()->getBucket($this->emptyInputBucketId);

        foreach ($this->clientWrapper->getTableAndFileStorageClient()->listBuckets() as $bucket) {
            if (preg_match('/^(c-)?[0-9]+-' . $emptyInputBucket['displayName'] . '$/ui', $bucket['name'])) {
                $this->clientWrapper->getTableAndFileStorageClient()->dropBucket(
                    $bucket['id'],
                    ['force' => true, 'async' => true],
                );
            }
        }

        $this->emptyBranchInputBucketId = $this->clientWrapper->getTableAndFileStorageClient()->createBucket(
            $this->devBranchId . '-' . $emptyInputBucket['displayName'],
            Client::STAGE_IN,
        );
    }

    protected function initEmptyRealBranchInputBucket(): void
    {
        $emptyInputBucket = $this->clientWrapper->getTableAndFileStorageClient()->getBucket($this->emptyInputBucketId);

        foreach ($this->clientWrapper->getTableAndFileStorageClient()->listBuckets() as $bucket) {
            if (preg_match('/^(c-)?[0-9]+-' . $emptyInputBucket['displayName'] . '$/ui', $bucket['name'])) {
                $this->clientWrapper->getTableAndFileStorageClient()->dropBucket(
                    $bucket['id'],
                    ['force' => true, 'async' => true],
                );
            }
        }

        $clientWraper = new ClientWrapper(
            $this->clientWrapper->getClientOptionsReadOnly()->setBranchId($this->devBranchId),
        );

        $this->emptyBranchInputBucketId = $clientWraper->getBranchClient()->createBucket(
            $emptyInputBucket['displayName'],
            Client::STAGE_IN,
        );
    }
}
