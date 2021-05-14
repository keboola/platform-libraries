<?php

namespace Keboola\InputMapping\File\Strategy;

use Exception;
use Keboola\InputMapping\Exception\FileNotFoundException;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\File\StrategyInterface;
use Keboola\InputMapping\Helper\BuildQueryFromConfigurationHelper;
use Keboola\InputMapping\Helper\ManifestCreator;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractStrategy implements StrategyInterface
{
    /** @var ClientWrapper */
    protected $clientWrapper;

    /** @var LoggerInterface */
    protected $logger;

    /** @var string */
    protected $destination;

    /** @var ManifestCreator */
    protected $manifestCreator;

    /** @var ProviderInterface */
    protected $dataStorage;

    /** @var ProviderInterface */
    protected $metadataStorage;

    /** @var InputFileStateList */
    protected $fileStateList;

    /** @var string */
    protected $format;

    public function __construct(
        ClientWrapper $storageClient,
        LoggerInterface $logger,
        ProviderInterface $dataStorage,
        ProviderInterface $metadataStorage,
        InputFileStateList $fileStateList,
        $format = 'json'
    ) {
        $this->clientWrapper = $storageClient;
        $this->logger = $logger;
        $this->manifestCreator = new ManifestCreator($this->clientWrapper->getBasicClient());
        $this->dataStorage = $dataStorage;
        $this->metadataStorage = $metadataStorage;
        $this->fileStateList = $fileStateList;
        $this->format = $format;
    }

    protected function ensurePathDelimiter($path)
    {
        return $this->ensureNoPathDelimiter($path) . '/';
    }

    protected function ensureNoPathDelimiter($path)
    {
        return rtrim($path, '\\/');
    }

    /**
     * @param string $destinationPath
     * @param string $fileId
     * @param string $fileName
     * @return string
     */
    abstract protected function getFileDestinationPath($destinationPath, $fileId, $fileName);

    /**
     * @param array $fileConfigurations
     * @param string $destination
     * @return InputFileStateList
     */
    public function downloadFiles($fileConfigurations, $destination)
    {
        $fileOptions = new GetFileOptions();
        $fileOptions->setFederationToken(true);
        $outputStateList = [];
        foreach ($fileConfigurations as $fileConfiguration) {
            $files = Reader::getFiles($fileConfiguration, $this->clientWrapper, $this->logger, $this->fileStateList);
            $biggestFileId = 0;
            try {
                $currentState = $this->fileStateList->getFile(
                    $this->fileStateList->getFileConfigurationIdentifier($fileConfiguration)
                );
                $outputStateConfiguration = [
                    'tags' => $currentState->getTags(),
                    'lastImportId' => $currentState->getLastImportId(),
                ];
            } catch (FileNotFoundException $e) {
                $outputStateConfiguration = [];
            }
            foreach ($files as $file) {
                $fileInfo = $this->clientWrapper->getBasicClient()->getFile($file['id'], $fileOptions);
                $fileDestinationPath = $this->getFileDestinationPath($destination, $fileInfo['id'], $fileInfo['name']);
                $overwrite = $fileConfiguration['overwrite'];

                if ($fileInfo['id'] > $biggestFileId) {
                    $outputStateConfiguration = [
                        'tags' => $this->fileStateList->getFileConfigurationIdentifier($fileConfiguration),
                        'lastImportId' => $fileInfo['id'],
                    ];
                    $biggestFileId = (int) $fileInfo['id'];
                }
                $this->logger->info(sprintf('Fetching file %s (%s).', $fileInfo['name'], $file['id']));
                try {
                    $this->downloadFile($fileInfo, $fileDestinationPath, $overwrite);
                } catch (Exception $e) {
                    throw new InputOperationException(
                        sprintf('Failed to download file %s (%s).', $fileInfo['name'], $file['id']),
                        0,
                        $e
                    );
                }
                $this->logger->info(sprintf('Fetched file %s (%s).', $fileInfo['name'], $file['id']));
            }
            if (!empty($outputStateConfiguration)) {
                $outputStateList[] = $outputStateConfiguration;
            }
        }
        $this->logger->info('All files were fetched.');
        return new InputFileStateList($outputStateList);
    }
}
