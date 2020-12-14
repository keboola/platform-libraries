<?php

namespace Keboola\InputMapping\File\Strategy;

use Exception;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\File\StrategyInterface;
use Keboola\InputMapping\Helper\ManifestWriter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\CapabilityInterface;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractStrategy implements StrategyInterface
{
    /** @var ClientWrapper */
    protected $clientWrapper;

    /** LoggerInterface */
    protected $logger;

    /** @var CapabilityInterface */
    protected $workspaceProvider;

    /** string */
    protected $destination;

    /** @var ManifestWriter */
    protected $manifestWriter;

    /** @var CapabilityInterface */
    protected $dataStorage;

    /** @var CapabilityInterface */
    protected $metadataStorage;

    public function __construct(
        ClientWrapper $storageClient,
        LoggerInterface $logger,
        CapabilityInterface $dataStorage,
        CapabilityInterface $metadataStorage,
        $destination,
        $format = 'json'
    ) {
        $this->clientWrapper = $storageClient;
        $this->logger = $logger;
        $this->destination = $destination;
        $this->manifestWriter = new ManifestWriter($this->clientWrapper->getBasicClient(), $format);
        $this->dataStorage = $dataStorage;
        $this->metadataStorage = $metadataStorage;
    }

    /**
     * @param array $fileConfigurations
     * @param string $destination
     */
    public function downloadFiles($fileConfigurations, $destination)
    {
        $fileOptions = new GetFileOptions();
        $fileOptions->setFederationToken(true);

        foreach ($fileConfigurations as $fileConfiguration) {
            $files = Reader::getFiles($fileConfiguration, $this->clientWrapper, $this->logger);
            foreach ($files as $file) {
                $fileInfo = $this->clientWrapper->getBasicClient()->getFile($file['id'], $fileOptions);
                $fileDestinationPath = sprintf('%s/%s_%s', $destination, $fileInfo['id'], $fileInfo["name"]);
                $this->logger->info(sprintf('Fetching file %s (%s).', $fileInfo['name'], $file['id']));
                try {
                    $this->downloadFile($fileInfo, $fileDestinationPath);
                } catch (Exception $e) {
                    throw new InputOperationException(
                        sprintf('Failed to download file %s (%s).', $fileInfo['name'], $file['id']),
                        0,
                        $e
                    );
                }
                $this->logger->info(sprintf('Fetched file %s (%s).', $fileInfo['name'], $file['id']));
            }
        }
        $this->logger->info('All files were fetched.');
    }
}
