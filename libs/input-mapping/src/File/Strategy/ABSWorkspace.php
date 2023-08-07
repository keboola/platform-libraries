<?php

declare(strict_types=1);

namespace Keboola\InputMapping\File\Strategy;

use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\Strategy\AbstractStrategy as AbstractFileStrategy;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;

class ABSWorkspace extends AbstractFileStrategy
{
    private ?BlobRestProxy $blobClient = null;

    /** @var ?array{container: string, connectionString: string} */
    private ?array $credentials = null;

    protected array $inputs = [];

    /**
     * @return array{container: string, connectionString: string}
     */
    private function getCredentials(): array
    {
        if (!$this->credentials) {
            $credentials = $this->dataStorage->getCredentials();
            if (empty($credentials['connectionString']) || empty($credentials['container'])) {
                throw new InputOperationException(
                    'Invalid credentials received: ' . implode(', ', array_keys($credentials))
                );
            }
            $this->credentials = $credentials;
        }
        return $this->credentials;
    }

    private function getBlobClient(): BlobRestProxy
    {
        if (!$this->blobClient) {
            $this->blobClient = ClientFactory::createClientFromConnectionString(
                $this->getCredentials()['connectionString']
            );
        }
        return $this->blobClient;
    }

    public function downloadFile(
        array $fileInfo,
        string $sourceBranchId,
        string $destinationPath,
        bool $overwrite
    ): void {
        $this->inputs[] = [
            'dataFileId' => $fileInfo['id'],
            'destination' => $destinationPath,
            'overwrite' => $overwrite,
        ];
        $manifest = $this->manifestCreator->createFileManifest($fileInfo);
        $adapter = new FileAdapter($this->format);
        $serializedManifest = $adapter->setConfig($manifest)->serialize();
        $manifestDestination = $destinationPath . '/' . $fileInfo['id'] . '.manifest';
        $this->writeFile($serializedManifest, $manifestDestination);
    }

    public function downloadFiles(array $fileConfigurations, string $destination): InputFileStateList
    {
        $inputFileStateList = parent::downloadFiles($fileConfigurations, $destination);
        if ($this->inputs) {
            $workspaces = new Workspaces($this->clientWrapper->getBranchClient());
            $workspaceId = $this->dataStorage->getWorkspaceId();
            foreach ($this->inputs as $input) {
                $workspaces->loadWorkspaceData((int) $workspaceId, [
                    'input' => [$input],
                    'preserve' => 1,
                ]);
            }
            $this->logger->info('All files were fetched.');
        }
        return $inputFileStateList;
    }

    private function writeFile(string $contents, string $destination): void
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

    protected function getFileDestinationPath(
        string $destinationPath,
        int $fileId,
        string $fileName
    ): string {
        /* Contrary to local strategy, in case of ABSWorkspace, the path is always a directory to which a
            file is exported with the name being fileId. */
        return sprintf(
            '%s/%s',
            $this->ensureNoPathDelimiter($destinationPath),
            $fileName
        );
    }
}
