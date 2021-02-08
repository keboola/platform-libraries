<?php

namespace Keboola\InputMapping\File\Strategy;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\Strategy\AbstractStrategy as AbstractFileStrategy;
use Keboola\InputMapping\File\StrategyInterface;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApiBranch\ClientWrapper;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use Psr\Log\LoggerInterface;

class ABSWorkspace extends AbstractFileStrategy implements StrategyInterface
{
    /** @var BlobRestProxy */
    private $blobClient;

    /** @var array container, connectionString */
    private $credentials;

    protected $inputs = [];

    public function __construct(ClientWrapper $storageClient, LoggerInterface $logger, ProviderInterface $dataStorage, ProviderInterface $metadataStorage, $format = 'json')
    {
        parent::__construct($storageClient, $logger, $dataStorage, $metadataStorage, $format);
    }

    /**
     * @return array credentials
     */
    private function getCredentials()
    {
        if (!$this->credentials) {
            $this->credentials = $this->dataStorage->getCredentials();
            if (empty($this->credentials['connectionString']) || empty($this->credentials['container'])) {
                throw new InputOperationException(
                    'Invalid credentials received: ' . implode(', ', array_keys($this->credentials))
                );
            }
        }
        return $this->credentials;
    }

    /**
     * @return BlobRestProxy
     */
    private function getBlobClient()
    {
        if (!$this->blobClient) {
            $this->blobClient = BlobRestProxy::createBlobService($this->getCredentials()['connectionString']);
        }
        return $this->blobClient;
    }

    public function downloadFile($fileInfo, $destinationPath)
    {
        $this->inputs[] = [
            'dataFileId' => $fileInfo['id'],
            'destination' => $destinationPath,
        ];
        $manifest = $this->manifestCreator->createFileManifest($fileInfo);
        $adapter = new FileAdapter($this->format);
        $serializedManifest = $adapter->setConfig($manifest)->serialize();
        $manifestDestination = $destinationPath . '/' . $fileInfo['id'] . '.manifest';
        $this->writeFile($serializedManifest, $manifestDestination);
    }

    public function downloadFiles($fileConfigurations, $destination)
    {
        parent::downloadFiles($fileConfigurations, $destination);
        if ($this->inputs) {
            $workspaces = new Workspaces($this->clientWrapper->getBasicClient());
            $workspaceId = $this->dataStorage->getWorkspaceId();
            foreach ($this->inputs as $input) {
                $workspaces->loadWorkspaceData($workspaceId, [
                    'input' => [$input],
                    'preserve' => 1,
                ]);
            }
            $this->logger->info('All files were fetched.');
        }
    }

    private function writeFile($contents, $destination)
    {
        try {
            $blobClient = $this->getBlobClient();
            $blobClient->createBlockBlob(
                $this->getCredentials()['container'],
                $destination,
                $contents
            );
        } catch (ServiceException $e) {
            throw new InvalidInputException(
                sprintf(
                    'Failed writing manifest to "%s" in container "%s".',
                    $destination,
                    $this->getCredentials()['container']
                ),
                $e->getCode(),
                $e
            );
        }
    }

    protected function getFileDestinationPath($destinationPath, $fileId, $fileName)
    {
        /* Contrary to local strategy, in case of ABSWorkspace, the path is always a directory to which a
            file is exported with the name being fileId. */
        return sprintf(
            '%s/%s',
            $this->ensureNoPathDelimiter($destinationPath),
            $fileName
        );
    }
}
