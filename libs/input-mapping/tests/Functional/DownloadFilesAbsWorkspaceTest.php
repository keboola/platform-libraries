<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Operation;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use Psr\Log\NullLogger;

class DownloadFilesAbsWorkspaceTest extends DownloadFilesTestAbstract
{
    private $runSynapseTests;

    /** @var string */
    protected $workspaceId;

    /** @var array [connectionString, container] */
    protected $workspaceCredentials;

    public function setUp()
    {
        $this->runSynapseTests = getenv('RUN_SYNAPSE_TESTS');
        if (!$this->runSynapseTests) {
            return;
        }
        if (getenv('SYNAPSE_STORAGE_API_TOKEN') === false) {
            throw new Exception('SYNAPSE_STORAGE_API_TOKEN must be set for synapse tests');
        }
        if (getenv('SYNAPSE_STORAGE_API_URL') === false) {
            throw new Exception('SYNAPSE_STORAGE_API_URL must be set for synapse tests');
        }
        parent::setUp();
    }

    public function tearDown()
    {
        if ($this->workspaceId) {
            $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
            $workspaces->deleteWorkspace($this->workspaceId);
            $this->workspaceId = null;
        }
        parent::tearDown();
    }

    protected function initClient()
    {
        $token = (string) getenv('SYNAPSE_STORAGE_API_TOKEN');
        $url = (string) getenv('SYNAPSE_STORAGE_API_URL');
        $this->clientWrapper = new ClientWrapper(
            new Client(["token" => $token, "url" => $url]),
            null,
            null
        );
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

    protected function getStagingFactory($clientWrapper = null, $format = 'json', $logger = null)
    {
        $stagingFactory = new StrategyFactory(
            $clientWrapper ? $clientWrapper : $this->clientWrapper,
            $logger ? $logger : new NullLogger(),
            $format
        );
        $mockWorkspace = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getWorkspaceId'])
            ->getMock();
        $mockWorkspace->method('getWorkspaceId')->willReturnCallback(
            function () {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
                    $workspace = $workspaces->createWorkspace(['backend' => 'abs']);
                    $this->workspaceId = $workspace['id'];
                    $this->workspaceCredentials = $workspace['connection'];
                }
                return $this->workspaceId;
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
        /** @var ProviderInterface $mockWorkspace */
        $stagingFactory->addProvider(
            $mockWorkspace,
            [
                StrategyFactory::WORKSPACE_ABS => new Operation([Operation::FILE_DATA])
            ]
        );
        /** @var ProviderInterface $mockLocal */
        $stagingFactory->addProvider(
            $mockLocal,
            [
                StrategyFactory::WORKSPACE_ABS => new Operation([Operation::FILE_METADATA])
            ]
        );
        return $stagingFactory;
    }

    public function testAbsReadFiles()
    {
        $this->clientWrapper->setBranchId('');

        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");

        $id1 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags(["download-files-test"])
        );
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile(
            $root . "/upload",
            (new FileUploadOptions())->setTags(["download-files-test"])
        );
        sleep(5);
        $reader = new Reader($this->getStagingFactory());
        $configuration = [["tags" => ["download-files-test"]]];
        $reader->downloadFiles($configuration, 'data/in/files/', StrategyFactory::WORKSPACE_ABS);

        $blobClient = BlobRestProxy::createBlobService($this->workspaceCredentials['connectionString']);
        $blobResult1 = $blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'data/in/files/' . $id1 . '_upload/' . $id1
        );
        $blobResult2 = $blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'data/in/files/' . $id2 . '_upload/' . $id2
        );

        self::assertEquals("test", stream_get_contents($blobResult1->getContentStream()));
        self::assertEquals("test", stream_get_contents($blobResult2->getContentStream()));

        $adapter = new Adapter();
        $manifest1 = $adapter->readFromFile($root . "/data/in/files/" . $id1 . "_upload.manifest");
        $manifest2 = $adapter->readFromFile($root . "/data/in/files/" . $id2 . "_upload.manifest");

        self::assertArrayHasKey('id', $manifest1);
        self::assertArrayHasKey('name', $manifest1);
        self::assertArrayHasKey('created', $manifest1);
        self::assertArrayHasKey('is_public', $manifest1);
        self::assertArrayHasKey('is_encrypted', $manifest1);
        self::assertArrayHasKey('tags', $manifest1);
        self::assertArrayHasKey('max_age_days', $manifest1);
        self::assertArrayHasKey('size_bytes', $manifest1);
        self::assertArrayHasKey('is_sliced', $manifest1);
        self::assertFalse($manifest1['is_sliced']);
        self::assertEquals($id1, $manifest1["id"]);
        self::assertEquals($id2, $manifest2["id"]);
    }

    public function testReadFilesTagsFilterRunId()
    {
        $this->clientWrapper->setBranchId('');

        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->getStagingFactory());
        $fo = new FileUploadOptions();
        $fo->setTags(["download-files-test"]);

        $this->clientWrapper->getBasicClient()->setRunId('xyz');
        $id1 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $this->clientWrapper->getBasicClient()->setRunId('1234567');
        $id3 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id4 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $this->clientWrapper->getBasicClient()->setRunId('1234567.8901234');
        $id5 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id6 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        sleep(5);
        $configuration = [["tags" => ["download-files-test"], "filter_by_run_id" => true]];
        $reader->downloadFiles($configuration, 'download', StrategyFactory::WORKSPACE_ABS);

        $blobClient = BlobRestProxy::createBlobService($this->workspaceCredentials['connectionString']);
        try {
            $this->assertEmpty($blobClient->getBlob(
                $this->workspaceCredentials['container'],
                $root . "/download/" . $id1 . '_upload/' . $id1
            ));
        } catch (ServiceException $exception) {
            $this->assertEquals(404, $exception->getCode());
        }
        try {
            $this->assertEmpty($blobClient->getBlob(
                $this->workspaceCredentials['container'],
                $root . "/download/" . $id2 . '_upload/' . $id2
            ));
        } catch (ServiceException $exception) {
            $this->assertEquals(404, $exception->getCode());
        }
        $this->assertNotEmpty($blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'download/' . $id3 . '_upload/' . $id3
        ));
        $this->assertNotEmpty($blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'download/' . $id4 . '_upload/' . $id4
        ));
        $this->assertNotEmpty($blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'download/' . $id5 . '_upload/' . $id5
        ));
        $this->assertNotEmpty($blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'download/' . $id6 . '_upload/' . $id6
        ));
    }

    public function testReadFilesEsQueryFilterRunId()
    {
        $this->clientWrapper->setBranchId('');

        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $root = $this->tmpDir;
        file_put_contents($root . "/upload", "test");
        $reader = new Reader($this->getStagingFactory());
        $fo = new FileUploadOptions();
        $fo->setTags(["download-files-test"]);

        $this->clientWrapper->getBasicClient()->setRunId('xyz');
        $id1 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id2 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $this->clientWrapper->getBasicClient()->setRunId('1234567');
        $id3 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id4 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $this->clientWrapper->getBasicClient()->setRunId('1234567.8901234');
        $id5 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        $id6 = $this->clientWrapper->getBasicClient()->uploadFile($root . "/upload", $fo);
        sleep(5);
        $configuration = [["query" => "tags: download-files-test", "filter_by_run_id" => true]];
        $reader->downloadFiles($configuration, 'download', StrategyFactory::WORKSPACE_ABS);

        $blobClient = BlobRestProxy::createBlobService($this->workspaceCredentials['connectionString']);
        try {
            $this->assertEmpty($blobClient->getBlob(
                $this->workspaceCredentials['container'],
                $root . "/download/" . $id1 . '_upload/'  . $id1
            ));
        } catch (ServiceException $exception) {
            $this->assertEquals(404, $exception->getCode());
        }
        try {
            $this->assertEmpty($blobClient->getBlob(
                $this->workspaceCredentials['container'],
                $root . "/download/" . $id2 . '_upload/' . $id2
            ));
        } catch (ServiceException $exception) {
            $this->assertEquals(404, $exception->getCode());
        }
        $this->assertNotEmpty($blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'download/' . $id3 . '_upload/' . $id3
        ));
        $this->assertNotEmpty($blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'download/' . $id4 . '_upload/' . $id4
        ));
        $this->assertNotEmpty($blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'download/' . $id5 . '_upload/' . $id5
        ));
        $this->assertNotEmpty($blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'download/' . $id6 . '_upload/' . $id6
        ));
    }
}
