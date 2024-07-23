<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class DownloadFilesAbsWorkspaceTest extends TestCase
{
    protected ?string $workspaceId;

    /** @var array [connectionString, container] */
    protected array $workspaceCredentials;
    protected BlobRestProxy $blobClient;

    protected function getClientWrapper(?string $branchId): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('SYNAPSE_STORAGE_API_URL'),
                (string) getenv('SYNAPSE_STORAGE_API_TOKEN'),
                $branchId,
            ),
        );
    }

    public function setUp(): void
    {
        $runSynapseTests = (string) getenv('RUN_SYNAPSE_TESTS');
        if (!$runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        if (getenv('SYNAPSE_STORAGE_API_TOKEN') === false) {
            throw new Exception('SYNAPSE_STORAGE_API_TOKEN must be set for synapse tests');
        }
        if (getenv('SYNAPSE_STORAGE_API_URL') === false) {
            throw new Exception('SYNAPSE_STORAGE_API_URL must be set for synapse tests');
        }
        parent::setUp();
        $this->getStagingFactory($this->getClientWrapper(null))
            ->getStrategyMap()[AbstractStrategyFactory::WORKSPACE_ABS]
            ->getFileDataProvider()?->getWorkspaceId(); //initialize the mock

        $this->blobClient = ClientFactory::createClientFromConnectionString(
            $this->workspaceCredentials['connectionString'],
        );
    }

    public function tearDown(): void
    {
        if ($this->workspaceId) {
            $workspaces = new Workspaces($this->getClientWrapper(null)->getBranchClient());
            $workspaces->deleteWorkspace((int) $this->workspaceId, [], true);
            $this->workspaceId = null;
        }
        parent::tearDown();
    }

    protected function getStagingFactory(
        ClientWrapper $clientWrapper,
        string $format = 'json',
        ?LoggerInterface $logger = null,
    ): StrategyFactory {
        $stagingFactory = new StrategyFactory(
            $clientWrapper,
            $logger ?: new NullLogger(),
            $format,
        );
        $mockWorkspace = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getWorkspaceId', 'getCredentials'])
            ->getMock();
        $mockWorkspace->method('getWorkspaceId')->willReturnCallback(
            function () use ($clientWrapper) {
                if (empty($this->workspaceId)) {
                    $workspaces = new Workspaces($clientWrapper->getBranchClient());
                    $workspace = $workspaces->createWorkspace(['backend' => 'abs'], true);
                    $this->workspaceId = (string) $workspace['id'];
                    $this->workspaceCredentials = $workspace['connection'];
                }
                return $this->workspaceId;
            },
        );
        $mockWorkspace->method('getCredentials')->willReturn($this->workspaceCredentials ?? []);

        /** @var ProviderInterface $mockWorkspace */
        $stagingFactory->addProvider(
            $mockWorkspace,
            [
                AbstractStrategyFactory::WORKSPACE_ABS => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA]),
            ],
        );
        return $stagingFactory;
    }

    private function assertBlobNotEmpty(string $blobPath): void
    {
        self::assertNotEmpty(
            stream_get_contents(
                $this->blobClient->getBlob(
                    $this->workspaceCredentials['container'],
                    $blobPath,
                )->getContentStream(),
            ),
        );
    }

    public function testAbsReadFiles(): void
    {
        $this->blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'data/in/tables/sometable.csv',
            'some data',
        );

        $temp = new Temp('input-mapping');
        $root = $temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        $clientWrapper = $this->getClientWrapper(null);
        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags(['download-files-test']),
        );
        $id2 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags(['download-files-test']),
        );
        sleep(3);
        $reader = new Reader($this->getStagingFactory($clientWrapper));
        $configuration = [['tags' => ['download-files-test'], 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'data/in/files/',
            AbstractStrategyFactory::WORKSPACE_ABS,
            new InputFileStateList([]),
        );

        $blobResult1 = $this->blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'data/in/files/upload/' . $id1,
        );
        $manifestResult1 = $this->blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'data/in/files/upload/' . $id1 . '.manifest',
        );
        $blobResult2 = $this->blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'data/in/files/upload/' . $id2,
        );
        $manifestResult2 = $this->blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'data/in/files/upload/' . $id2 . '.manifest',
        );

        self::assertEquals('test', stream_get_contents($blobResult1->getContentStream()));
        self::assertEquals('test', stream_get_contents($blobResult2->getContentStream()));

        $manifest1 = json_decode((string) stream_get_contents($manifestResult1->getContentStream()), true);
        $manifest2 = json_decode((string) stream_get_contents($manifestResult2->getContentStream()), true);
        self::assertIsArray($manifest1);
        self::assertIsArray($manifest2);
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
        self::assertEquals($id1, $manifest1['id']);
        self::assertEquals($id2, $manifest2['id']);

        // verify that the workspace contents were preserved
        $blobResult = $this->blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'data/in/tables/sometable.csv',
        );
        self::assertEquals('some data', stream_get_contents($blobResult->getContentStream()));
    }

    public function testAbsReadFilesOverwrite(): void
    {
        $temp = new Temp('input-mapping');
        $root = $temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');

        $clientWrapper = $this->getClientWrapper(null);
        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $root . '/upload',
            (new FileUploadOptions())->setTags(['download-files-test']),
        );
        sleep(3);
        $reader = new Reader($this->getStagingFactory($clientWrapper));

        // upload file for the first time
        $configuration = [['tags' => ['download-files-test'], 'overwrite' => true]];
        $reader->downloadFiles(
            $configuration,
            'data/in/files/',
            AbstractStrategyFactory::WORKSPACE_ABS,
            new InputFileStateList([]),
        );
        $blobResult1 = $this->blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'data/in/files/upload/' . $id1,
        );
        self::assertEquals('test', stream_get_contents($blobResult1->getContentStream()));

        // modify file contents
        $this->blobClient->createBlockBlob(
            $this->workspaceCredentials['container'],
            'data/in/files/upload/' . $id1,
            'some overwritten data',
        );

        // upload file for the second time
        $reader->downloadFiles(
            $configuration,
            'data/in/files/',
            AbstractStrategyFactory::WORKSPACE_ABS,
            new InputFileStateList([]),
        );
        $blobResult1 = $this->blobClient->getBlob(
            $this->workspaceCredentials['container'],
            'data/in/files/upload/' . $id1,
        );
        // should be overwritten back to what it was
        self::assertEquals('test', stream_get_contents($blobResult1->getContentStream()));

        // upload file for the third time, should fail now
        $configuration = [['tags' => ['download-files-test'], 'overwrite' => false]];
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('already exists in workspace');
        $reader->downloadFiles(
            $configuration,
            'data/in/files/',
            AbstractStrategyFactory::WORKSPACE_ABS,
            new InputFileStateList([]),
        );
    }

    public function testAbsWorkspaceAdaptiveInput(): void
    {
        $clientWrapper = $this->getClientWrapper(null);
        $temp = new Temp('input-mapping');
        $root = $temp->getTmpFolder();
        file_put_contents($root . '/upload', 'test');
        $reader = new Reader($this->getStagingFactory($clientWrapper));
        $fo = new FileUploadOptions();
        $fo->setTags(['download-files-test']);

        $id1 = $clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        $id2 = $clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        sleep(2);
        $configuration = [[
            'tags' => ['download-files-test'],
            'changed_since' => InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE,
            'overwrite' => true,
        ]];
        $outputFileStateList = $reader->downloadFiles(
            $configuration,
            'download',
            AbstractStrategyFactory::WORKSPACE_ABS,
            new InputFileStateList([]),
        );
        $tagList = [
            [
                'name' => 'download-files-test',
            ],
        ];
        $lastFileState = $outputFileStateList->getFile($tagList);
        self::assertEquals($id2, $lastFileState->getLastImportId());
        // make sure the files are there
        $this->assertBlobNotEmpty(
            'download/upload/' . $id1,
        );
        $this->assertBlobNotEmpty(
            'download/upload/' . $id1 . '.manifest',
        );
        $this->assertBlobNotEmpty(
            'download/upload/' . $id2,
        );
        $this->assertBlobNotEmpty(
            'download/upload/' . $id2 . '.manifest',
        );

        $id3 = $clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        $id4 = $clientWrapper->getTableAndFileStorageClient()->uploadFile($root . '/upload', $fo);
        sleep(2);

        $newOutputFileStateList = $reader->downloadFiles(
            $configuration,
            'download-adaptive',
            AbstractStrategyFactory::WORKSPACE_ABS,
            $outputFileStateList,
        );
        $lastFileState = $newOutputFileStateList->getFile($tagList);
        self::assertEquals($id4, $lastFileState->getLastImportId());
        try {
            $this->blobClient->getBlob(
                $this->workspaceCredentials['container'],
                'download-adaptive/upload/' . $id1,
            );
            self::fail('should have thrown 404');
        } catch (ServiceException $exception) {
            self::assertEquals(404, $exception->getCode());
        }
        try {
            $this->blobClient->getBlob(
                $this->workspaceCredentials['container'],
                'download-adaptive/upload/' . $id1 . '.manifest',
            );
            self::fail('should have thrown 404');
        } catch (ServiceException $exception) {
            self::assertEquals(404, $exception->getCode());
        }
        try {
            $this->blobClient->getBlob(
                $this->workspaceCredentials['container'],
                $root . 'download-adaptive/upload/' . $id2,
            );
            self::fail('should have thrown 404');
        } catch (ServiceException $exception) {
            self::assertEquals(404, $exception->getCode());
        }
        try {
            $this->blobClient->getBlob(
                $this->workspaceCredentials['container'],
                'download-adaptive/uppload/' . $id2 . '.manifest',
            );
            self::fail('should have thrown 404');
        } catch (ServiceException $exception) {
            self::assertEquals(404, $exception->getCode());
        }
        $this->assertBlobNotEmpty(
            'download-adaptive/upload/' . $id3,
        );
        $this->assertBlobNotEmpty(
            'download-adaptive/upload/' . $id3 . '.manifest',
        );
        $this->assertBlobNotEmpty(
            'download-adaptive/upload/' . $id4,
        );
        $this->assertBlobNotEmpty(
            'download-adaptive/upload/' . $id4 . '.manifest',
        );
    }
}
