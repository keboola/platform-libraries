<?php

namespace Keboola\OutputMapping\Writer\Strategy\Files;

use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\OutputMapping\Configuration\File\Manifest\ABSWorkspaceFileAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use Psr\Log\LoggerInterface;

class AbsWorkspaceFilesStrategy extends AbstractFilesStrategy implements FilesStrategyInterface
{
    /** @var BlobRestProxy */
    private $blobClient;

    /** @var string */
    private $container;

    public function __construct(ClientWrapper $storageClient, LoggerInterface $logger, WorkspaceProviderInterface $workspaceProvider, $format = 'json')
    {
        parent::__construct($storageClient, $logger, $workspaceProvider, $format);
        $credentials = $this->workspaceProvider->getCredentials(WorkspaceProviderInterface::TYPE_ABS);
        $this->blobClient = BlobRestProxy::createBlobService($credentials['connectionString']);
        $this->container = $credentials['container'];
    }

    public function getManifestFiles($dir)
    {
        $blobListOptions = new ListBlobsOptions();
        $blobListOptions->setPrefix($dir);
        $blobListResult = $this->blobClient->listBlobs($this->container, $blobListOptions);
        foreach ($blobListResult->getBlobs() as $blob) {
            if (substr( $blob->getName(), -strlen('.manifest')) === '.manifest') {
                $manifestFileNames[] = $blob->getName();
            }
        }
        return $manifestFileNames;
    }

    public function readFileManifest($manifestFile)
    {
        $adapter = new ABSWorkspaceFileAdapter($this->format);
        try {
            return $adapter->readFromFile($manifestFile);
        } catch (\Exception $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Failed to parse manifest file "%s" as "%s": %s',
                    $manifestFile,
                    $this->format,
                    $e->getMessage()
                ),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @inheritDoc
     */
    public function getFiles($dir)
    {
        $blobListOptions = new ListBlobsOptions();
        $blobListOptions->setPrefix($dir);
        $blobListResult = $this->blobClient->listBlobs($this->container, $blobListOptions);
        $files = [];
        foreach ($blobListResult->getBlobs() as $blob) {
            if (substr( $blob->getName(), -strlen('.manifest')) !== '.manifest') {
                $files[] = (new File())->setFileName($blob->getName())->setPath($blob->getUrl());
            }
        }
        return $files;
    }

    /**
     * @inheritDoc
     */
    public function uploadFile($file, array $storageConfig = [])
    {
        // Since we do not yet have the ability to load files directly from ABS workspace to Sapi
        // we will first download it locally and then upload
        $blobResult = $this->blobClient->getBlob($this->container, $file);

        $tmp = new Temp();
        $tmpdir = $tmp->getTmpFolder();
        file_put_contents($tmpdir . $file, stream_get_contents($blobResult->getContentStream()));
        $options = new FileUploadOptions();
        $options
            ->setTags(array_unique($storageConfig['tags']))
            ->setIsPermanent($storageConfig['is_permanent'])
            ->setIsEncrypted($storageConfig['is_encrypted'])
            ->setIsPublic($storageConfig['is_public'])
            ->setNotify($storageConfig['notify']);
        $this->clientWrapper->getBasicClient()->uploadFile($tmpdir . $file, $options);
    }
}
