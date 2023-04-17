<?php

declare(strict_types=1);

namespace Keboola\InputMapping\File\Strategy;

use Keboola\InputMapping\Configuration\Adapter;
use Keboola\InputMapping\Exception\FileNotFoundException;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\File\StrategyInterface;
use Keboola\InputMapping\Helper\ManifestCreator;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;
use Throwable;

abstract class AbstractStrategy implements StrategyInterface
{
    protected string $destination;
    protected ManifestCreator $manifestCreator;

    /**
     * @param Adapter::FORMAT_YAML | Adapter::FORMAT_JSON $format
     */
    public function __construct(
        protected readonly ClientWrapper $clientWrapper,
        protected readonly LoggerInterface $logger,
        protected readonly ProviderInterface $dataStorage,
        protected readonly ProviderInterface $metadataStorage,
        protected readonly InputFileStateList $fileStateList,
        protected readonly string $format = Adapter::FORMAT_JSON
    ) {
        $this->manifestCreator = new ManifestCreator();
    }

    protected function ensurePathDelimiter(string $path): string
    {
        return $this->ensureNoPathDelimiter($path) . '/';
    }

    protected function ensureNoPathDelimiter(string $path): string
    {
        return rtrim($path, '\\/');
    }

    abstract protected function getFileDestinationPath(
        string $destinationPath,
        int $fileId,
        string $fileName
    ): string;

    public function downloadFiles(array $fileConfigurations, string $destination): InputFileStateList
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
            } catch (FileNotFoundException) {
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
                try {
                    $this->downloadFile($fileInfo, $fileDestinationPath, $overwrite);
                } catch (Throwable $e) {
                    throw new InputOperationException(
                        sprintf(
                            'Failed to download file %s (%s): %s',
                            $fileInfo['name'],
                            $file['id'],
                            $e->getMessage()
                        ),
                        0,
                        $e
                    );
                }
                $this->logger->info(sprintf('Fetched file "%s".', basename($fileDestinationPath)));
            }
            if (!empty($outputStateConfiguration)) {
                $outputStateList[] = $outputStateConfiguration;
            }
        }
        $this->logger->info('All files were fetched.');
        return new InputFileStateList($outputStateList);
    }
}
