<?php

namespace Keboola\OutputMapping\Writer\File\Strategy;

use Exception;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Configuration\File\Manifest\Adapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Writer\File\FileItem;
use Keboola\OutputMapping\Writer\File\StrategyInterface;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use Psr\Log\LoggerInterface;

class ABSWorkspace extends AbstractFileStrategy implements StrategyInterface
{
    /* Maximum limit is 5000 https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs,
        Since paging is not implemented, leave this at lower value so that it can be raised as a quick fix before
        paging needs to be implemented. */
    const MAX_RESULTS = 1000;

    /** @var BlobRestProxy */
    private $blobClient;

    /** @var string */
    private $container;

    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        ProviderInterface $dataStorage,
        ProviderInterface $metadataStorage,
        $format
    ) {
        parent::__construct($clientWrapper, $logger, $dataStorage, $metadataStorage, $format);
        $credentials = $this->dataStorage->getCredentials();
        if (empty($credentials['connectionString']) || empty($credentials['container'])) {
            throw new OutputOperationException(
                'Invalid credentials received: ' . implode(', ', array_keys($credentials))
            );
        }
        $this->blobClient = BlobRestProxy::createBlobService($credentials['connectionString']);
        $this->container = $credentials['container'];
    }

    /**
     * @param $dir
     * @return Blob[]
     */
    private function listBlobs($dir)
    {
        $dir = trim($dir, '/') . '/';
        try {
            $blobListOptions = new ListBlobsOptions();
            $blobListOptions->setPrefix($dir);
            $blobListOptions->setMaxResults(self::MAX_RESULTS);
            $blobListResult = $this->blobClient->listBlobs($this->container, $blobListOptions);
            if (count($blobListResult->getBlobs()) === self::MAX_RESULTS) {
                // Paging not implemented yet
                throw new OutputOperationException('Maximum number of files in workspace reached.');
            }
            return $blobListResult->getBlobs();
        } catch (ServiceException $e) {
            throw new InvalidOutputException(
                sprintf('Failed to list files: "%s".', $e->getErrorText()),
                $e->getCode(),
                $e
            );
        }
    }

    /** @inheritDoc */
    public function listFiles($dir)
    {
        $files = [];
        foreach ($this->listBlobs($dir) as $blob) {
            if (substr($blob->getName(), -strlen('.manifest')) !== '.manifest') {
                $files[$blob->getName()] = new FileItem($blob->getName(), dirname($blob->getName()), basename($blob->getName()));
            }
        }
        return $files;
    }

    /** @inheritDoc */
    public function listManifests($dir)
    {
        $manifestFileNames = [];
        foreach ($this->listBlobs($dir) as $blob) {
            if (substr($blob->getName(), -strlen('.manifest')) === '.manifest') {
                $manifestFileNames[$blob->getName()] = new FileItem($blob->getName(), dirname($blob->getName()), basename($blob->getName()));
            }
        }
        return $manifestFileNames;
    }

    /** @inheritDoc */
    public function loadFileToStorage($file, array $storageConfig)
    {
        // Since we do not yet have the ability to load files directly from ABS workspace to Sapi
        // we will first download it locally and then upload
        if (empty($file)) {
            // if the file is empty, ABS throw "unauthorized" exception, which is confusing
            throw new InvalidOutputException(sprintf('File "%s" is empty.', var_export($file, true)));
        }
        try {
            $blobResult = $this->blobClient->getBlob($this->container, $file);
        } catch (ServiceException $e) {
            throw new InvalidOutputException(
                sprintf('File "%s" does not exist in container "%s".', $file, $this->container),
                $e->getCode(),
                $e
            );
        }

        $tmp = new Temp();
        $tmp->initRunFolder();
        $tmpFileName = $tmp->getTmpFolder()  . '/' . basename($file);
        if (($destination = fopen($tmpFileName, 'w')) !== false) {
            if (stream_copy_to_stream($blobResult->getContentStream(), $destination) === false) {
                throw new OutputOperationException(sprintf('Failed to copy stream to "%s"', $destination));
            }
            fclose($destination);
        } else {
            throw new OutputOperationException(sprintf('Failed to open stream "%s".', $tmpFileName));
        }

        $storageConfig = $this->preProcessStorageConfig($storageConfig);
        $options = new FileUploadOptions();
        $options
            ->setTags(array_unique($storageConfig['tags']))
            ->setIsPermanent($storageConfig['is_permanent'])
            ->setIsEncrypted($storageConfig['is_encrypted'])
            ->setIsPublic($storageConfig['is_public'])
            ->setNotify($storageConfig['notify']);
        return $this->clientWrapper->getBasicClient()->uploadFile($tmpFileName, $options);
    }

    /** @inheritDoc */
    public function readFileManifest($manifestFile)
    {
        $adapter = new Adapter($this->format);
        try {
            $blobResult = $this->blobClient->getBlob($this->container, $manifestFile);
        } catch (ServiceException $e) {
            throw new InvalidOutputException(
                sprintf('Failed to read manifest "%s": "%s', $manifestFile, $e->getErrorText()),
                $e->getCode(),
                $e
            );
        }
        try {
            $contents = stream_get_contents($blobResult->getContentStream());
            return $adapter->deserialize($contents);
        } catch (Exception $e) {
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
}
