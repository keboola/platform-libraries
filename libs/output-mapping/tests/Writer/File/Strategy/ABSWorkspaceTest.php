<?php

namespace Keboola\OutputMapping\Tests\Writer\File\Strategy;

use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Tests\InitSynapseStorageClientTrait;
use Keboola\OutputMapping\Tests\Writer\Workspace\BaseWriterWorkspaceTest;
use Keboola\OutputMapping\Writer\File\Strategy\ABSWorkspace;
use Keboola\StorageApi\Workspaces;
use Keboola\Temp\Temp;
use Psr\Log\Test\TestLogger;
use stdClass;
use Symfony\Component\Yaml\Yaml;

class ABSWorkspaceTest extends BaseWriterWorkspaceTest
{
    use InitSynapseStorageClientTrait;

    /** @var Temp */
    private $temp;

    protected function initClient($branchId = '')
    {
        $this->clientWrapper = $this->getSynapseClientWrapper();
    }

    public function setUp(): void
    {
        if (!$this->checkSynapseTests()) {
            self::markTestSkipped('Synapse tests disabled.');
        }
        $this->temp = new Temp();
        $this->temp->initRunFolder();
        parent::setUp();
    }

    private function getProvider(array $data = [])
    {
        $mock = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getWorkspaceId', 'getCredentials'])
            ->getMock();
        $mock->method('getWorkspaceId')->willReturnCallback(
            function () use ($data) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
                    $workspace = $workspaces->createWorkspace(['backend' => 'abs'], true);
                    $this->workspaceId = $workspace['id'];
                    $this->workspace = $data ? $data : $workspace;
                }
                return $this->workspaceId;
            }
        );
        $mock->method('getCredentials')->willReturnCallback(
            function () use ($data) {
                if (!$this->workspaceId) {
                    $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
                    $workspace = $workspaces->createWorkspace(['backend' => 'abs'], true);
                    $this->workspaceId = $workspace['id'];
                    $this->workspace = $data ? $data : $workspace;
                }
                return $this->workspace['connection'];
            }
        );
        /** @var ProviderInterface $mock */
        return $mock;
    }

    public function testCreateStrategyInvalidWorkspace()
    {
        self::expectException(OutputOperationException::class);
        self::expectExceptionMessage('Invalid credentials received: foo, bar');
        new ABSWorkspace(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(['connection' => ['foo' => 'bar', 'bar' => 'Kochba']]),
            $this->getProvider(['connection' => ['foo' => 'bar', 'bar' => 'Kochba']]),
            'json'
        );
    }

    public function testListFilesNoFiles()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        $files = $strategy->listFiles('data/out/files');
        self::assertSame([], $files);
    }

    public function testListFilesWorkspaceDropped()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
        $workspaces->deleteWorkspace($this->workspace['id']);
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage('Failed to list files: "The specified container does not exist.".');
        $strategy->listFiles('data/out/files');
    }

    public function testListFiles()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspace['connection']['connectionString']);
        $blobClient->createAppendBlob($this->workspace['connection']['container'], 'data/out/files/my-file');
        $blobClient->createAppendBlob($this->workspace['connection']['container'], 'data/out/files/my-file.manifest');
        $blobClient->createAppendBlob($this->workspace['connection']['container'], 'data/out/files/my-second-file');
        $blobClient->createAppendBlob($this->workspace['connection']['container'], 'data/out/files/my-second-file.manifest');
        $blobClient->createAppendBlob($this->workspace['connection']['container'], 'data/out/tables/my-other-file');
        $files = $strategy->listFiles('data/out/files');
        $fileNames = [];
        foreach ($files as $file) {
            $fileNames[$file->getPathName()] = $file->getPath();
        }
        self::assertEquals(['data/out/files/my-file', 'data/out/files/my-second-file'], array_keys($fileNames));
        self::assertStringEndsWith('data/out/files', $fileNames['data/out/files/my-file']);
    }

    public function testListFilesMaxItems()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspace['connection']['connectionString']);
        for ($i = 0; $i < 1000; $i++) {
            $blobClient->createAppendBlob($this->workspace['connection']['container'], 'data/out/files/my-file' . $i);
        }
        self::expectException(OutputOperationException::class);
        self::expectExceptionMessage('Maximum number of files in workspace reached.');
        $strategy->listFiles('data/out/files');
    }

    public function testListManifestsWorkspaceDropped()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
        $workspaces->deleteWorkspace($this->workspace['id']);
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage('Failed to list files: "The specified container does not exist.".');
        $strategy->listManifests('data/out/files');
    }

    public function testListManifests()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspace['connection']['connectionString']);
        $blobClient->createAppendBlob($this->workspace['connection']['container'], 'data/out/files/my-file');
        $blobClient->createAppendBlob($this->workspace['connection']['container'], 'data/out/files/my-file.manifest');
        $blobClient->createAppendBlob($this->workspace['connection']['container'], 'data/out/files/my-second-file');
        $blobClient->createAppendBlob($this->workspace['connection']['container'], 'data/out/files/my-second-file.manifest');
        $blobClient->createAppendBlob($this->workspace['connection']['container'], 'data/out/tables/my-other-file');
        $files = $strategy->listManifests('data/out/files');
        $fileNames = [];
        foreach ($files as $file) {
            $fileNames[$file->getPathName()] = $file->getPath();
        }
        self::assertEquals(['data/out/files/my-file.manifest', 'data/out/files/my-second-file.manifest'], array_keys($fileNames));
        self::assertStringEndsWith('data/out/files', $fileNames['data/out/files/my-file.manifest']);
    }

    public function testLoadFileToStorageEmptyConfig()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspace['connection']['connectionString']);
        $blobClient->createBlockBlob($this->workspace['connection']['container'], 'data/out/files/my-file_one', 'my-data');
        $blobClient->createBlockBlob($this->workspace['connection']['container'], 'data/out/files/my-file_one.manifest', 'manifest');
        $fileId = $strategy->loadFileToStorage('data/out/files/my-file_one', []);
        $this->clientWrapper->getBasicClient()->getFile($fileId);
        $destination = $this->temp->getTmpFolder() . 'destination';
        $this->clientWrapper->getBasicClient()->downloadFile($fileId, $destination);
        $contents = (string) file_get_contents($destination);
        self::assertEquals('my-data', $contents);

        $file = $this->clientWrapper->getBasicClient()->getFile($fileId);
        self::assertEquals($fileId, $file['id']);
        self::assertEquals('my_file_one', $file['name']);
        self::assertEquals([], $file['tags']);
        self::assertEquals(false, $file['isPublic']);
        self::assertEquals(true, $file['isEncrypted']);
        self::assertEquals(15, $file['maxAgeDays']);
    }

    public function testLoadFileToStorageFullConfig()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspace['connection']['connectionString']);
        $blobClient->createBlockBlob($this->workspace['connection']['container'], 'data/out/files/my-file_one', 'my-data');
        $blobClient->createBlockBlob($this->workspace['connection']['container'], 'data/out/files/my-file_one.manifest', 'manifest');
        $fileId = $strategy->loadFileToStorage(
            'data/out/files/my-file_one',
            [
                'notify' => false,
                'tags' => ['first-tag', 'second-tag'],
                'is_public' => true,
                'is_permanent' => true,
                'is_encrypted' => true,
            ]
        );
        $this->clientWrapper->getBasicClient()->getFile($fileId);
        $destination = $this->temp->getTmpFolder() . 'destination';
        $this->clientWrapper->getBasicClient()->downloadFile($fileId, $destination);
        $contents = (string)file_get_contents($destination);
        self::assertEquals('my-data', $contents);

        $file = $this->clientWrapper->getBasicClient()->getFile($fileId);
        self::assertEquals($fileId, $file['id']);
        self::assertEquals('my_file_one', $file['name']);
        self::assertEquals(['first-tag', 'second-tag'], $file['tags']);
        self::assertEquals(false, $file['isPublic']);
        self::assertEquals(true, $file['isEncrypted']);
        self::assertEquals(null, $file['maxAgeDays']);
    }

    public function testLoadFileToStorageFileDoesNotExist()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage('File "data/out/files/my-file_one" does not exist in container "' . $this->workspace['connection']['container'] . '".');
        $strategy->loadFileToStorage('data/out/files/my-file_one', []);
    }

    public function testLoadFileToStorageFileNameEmpty()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage('File "\'\'" is empty.');
        $strategy->loadFileToStorage('', []);
    }

    public function testReadFileManifestFull()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspace['connection']['connectionString']);
        $blobClient->createBlockBlob($this->workspace['connection']['container'], 'data/out/files/my-file_one', 'my-data');
        $sourceData = [
            'is_public' => true,
            'is_permanent' => true,
            'is_encrypted' => true,
            'notify' => false,
            'tags' => [
                'my-first-tag',
                'second-tag'
            ]
        ];
        $blobClient->createBlockBlob(
            $this->workspace['connection']['container'],
            'data/out/files/my-file_one.manifest',
            json_encode($sourceData)
        );
        $manifestData = $strategy->readFileManifest('data/out/files/my-file_one.manifest');
        self::assertEquals(
            $sourceData,
            $manifestData
        );
    }

    public function testReadFileManifestFullYaml()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'yaml');
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspace['connection']['connectionString']);
        $blobClient->createBlockBlob($this->workspace['connection']['container'], 'data/out/files/my-file_one', 'my-data');
        $sourceData = [
            'is_public' => true,
            'is_permanent' => true,
            'is_encrypted' => true,
            'notify' => false,
            'tags' => [
                'my-first-tag',
                'second-tag'
            ]
        ];
        $blobClient->createBlockBlob(
            $this->workspace['connection']['container'],
            'data/out/files/my-file_one.manifest',
            Yaml::dump($sourceData)
        );
        $manifestData = $strategy->readFileManifest('data/out/files/my-file_one.manifest');
        self::assertEquals(
            $sourceData,
            $manifestData
        );
    }

    public function testReadFileManifestEmpty()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspace['connection']['connectionString']);
        $blobClient->createBlockBlob($this->workspace['connection']['container'], 'data/out/files/my-file_one', 'my-data');
        $expectedData = [
            'is_public' => false,
            'is_permanent' => false,
            'is_encrypted' => true,
            'notify' => false,
            'tags' => []
        ];
        $blobClient->createBlockBlob(
            $this->workspace['connection']['container'],
            'data/out/files/my-file_one.manifest',
            json_encode(new stdClass())
        );
        $manifestData = $strategy->readFileManifest('data/out/files/my-file_one.manifest');
        self::assertEquals(
            $expectedData,
            $manifestData
        );
    }

    public function testReadFileManifestNotExists()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage(
            'Failed to read manifest "data/out/files/my-file_one.manifest": "The specified blob does not exist.'
        );
        $strategy->readFileManifest('data/out/files/my-file_one.manifest');
    }

    public function testReadFileManifestInvalid()
    {
        $strategy = new ABSWorkspace($this->clientWrapper, new TestLogger(), $this->getProvider(), $this->getProvider(), 'json');
        $blobClient = ClientFactory::createClientFromConnectionString($this->workspace['connection']['connectionString']);
        $blobClient->createBlockBlob(
            $this->workspace['connection']['container'],
            'data/out/files/my-file_one.manifest',
            'not a valid json'
        );
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage(
            'Failed to parse manifest file "data/out/files/my-file_one.manifest" as "json": Syntax error'
        );
        $strategy->readFileManifest('data/out/files/my-file_one.manifest');
    }
}
