<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\File\Strategy;

use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Configuration\Adapter as ConfigurationAdapter;
use Keboola\OutputMapping\Configuration\File\Manifest\Adapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Writer\File\StrategyInterface;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\Blob;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use Psr\Log\LoggerInterface;
use Throwable;

class ABSWorkspace extends AbstractFileStrategy implements StrategyInterface
{
    /* Maximum limit is 5000 https://docs.microsoft.com/en-us/rest/api/storageservices/list-blobs,
        Since paging is not implemented, leave this at lower value so that it can be raised as a quick fix before
        paging needs to be implemented. */
    private const MAX_RESULTS = 1000;

    private BlobRestProxy $blobClient;
    private string $container;

    /**
     * @param ConfigurationAdapter::FORMAT_YAML | ConfigurationAdapter::FORMAT_JSON $format
     */
    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        ProviderInterface $dataStorage,
        ProviderInterface $metadataStorage,
        string $format,
    ) {
        parent::__construct($clientWrapper, $logger, $dataStorage, $metadataStorage, $format);
        $credentials = $this->dataStorage->getCredentials();
        if (empty($credentials['connectionString']) || empty($credentials['container'])) {
            throw new OutputOperationException(
                'Invalid credentials received: ' . implode(', ', array_keys($credentials)),
            );
        }
        $this->blobClient = ClientFactory::createClientFromConnectionString($credentials['connectionString']);
        $this->container = $credentials['container'];
    }

    /**
     * @return Blob[]
     */
    private function listBlobs(string $dir): array
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
                $e,
            );
        }
    }

    public function listFiles(string $dir): array
    {
        $files = [];
        foreach ($this->listBlobs($dir) as $blob) {
            if (!str_ends_with($blob->getName(), '.manifest')) {
                $files[$blob->getName()] = new FileItem(
                    $blob->getName(),
                    dirname($blob->getName()),
                    basename($blob->getName()),
                    false, // TODO really ?
                );
            }
        }
        return $files;
    }

    public function listManifests(string $dir): array
    {
        $manifestFileNames = [];
        foreach ($this->listBlobs($dir) as $blob) {
            if (str_ends_with($blob->getName(), '.manifest')) {
                $manifestFileNames[$blob->getName()] = new FileItem(
                    $blob->getName(),
                    dirname($blob->getName()),
                    basename($blob->getName()),
                    false,
                );
            }
        }
        return $manifestFileNames;
    }

    public function loadFileToStorage(string $file, array $storageConfig): string
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
                $e,
            );
        }

        $tmp = new Temp();
        $tmpFileName = $tmp->getTmpFolder()  . '/' . basename($file);
        $destination = fopen($tmpFileName, 'w');
        if ($destination !== false) {
            if (stream_copy_to_stream($blobResult->getContentStream(), $destination) === false) {
                throw new OutputOperationException(sprintf('Failed to copy stream to "%s"', $tmpFileName));
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
        return (string) $this->clientWrapper->getTableAndFileStorageClient()->uploadFile($tmpFileName, $options);
    }

    public function readFileManifest(string $manifestFile): array
    {
        $adapter = new Adapter($this->format);
        try {
            $blobResult = $this->blobClient->getBlob($this->container, $manifestFile);
        } catch (ServiceException $e) {
            throw new InvalidOutputException(
                sprintf('Failed to read manifest "%s": "%s', $manifestFile, $e->getErrorText()),
                $e->getCode(),
                $e,
            );
        }
        try {
            $contents = stream_get_contents($blobResult->getContentStream());
            return $adapter->deserialize((string) $contents);
        } catch (Throwable $e) {
            throw new InvalidOutputException(
                sprintf(
                    'Failed to parse manifest file "%s" as "%s": %s',
                    $manifestFile,
                    $this->format,
                    $e->getMessage(),
                ),
                $e->getCode(),
                $e,
            );
        }
    }
}
