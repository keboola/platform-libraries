<?php

namespace Keboola\InputMapping\File\Strategy;

use Exception;
use Keboola\InputMapping\Exception\FileNotFoundException;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\File\StrategyInterface;
use Keboola\InputMapping\Helper\BuildQueryFromConfigurationHelper;
use Keboola\InputMapping\Helper\ManifestCreator;
use Keboola\InputMapping\Helper\TagsRewriteHelper;
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
     */
    public function downloadFiles($fileConfigurations, $destination)
    {
        $fileOptions = new GetFileOptions();
        $fileOptions->setFederationToken(true);

        foreach ($fileConfigurations as $fileConfiguration) {
            // apply the state configuration limits
            if (isset($fileConfiguration['changed_since']) && !empty($fileConfiguration['changed_since'])) {
                if ($fileConfiguration['changed_since'] === InputTableOptions::ADAPTIVE_INPUT_MAPPING_VALUE) {
                    // get merged tags
                    $tags = (isset($fileConfiguration['tags']))
                        ? BuildQueryFromConfigurationHelper::getSourceTagsFromTags($fileConfiguration['tags'])
                        : $fileConfiguration['source']['tags'];
                    try {
                        $fileConfiguration['changed_since'] = $this->fileStateList
                            ->getFile($tags)
                            ->getLastImportId();
                    } catch (FileNotFoundException $e) {
                        // intentionally blank
                    }
                } else {
                    $fileConfiguration['changedSince'] = $this->definition['changed_since'];
                }
            }
            $files = Reader::getFiles($fileConfiguration, $this->clientWrapper, $this->logger);
            foreach ($files as $file) {
                $fileInfo = $this->clientWrapper->getBasicClient()->getFile($file['id'], $fileOptions);
                $fileDestinationPath = $this->getFileDestinationPath($destination, $fileInfo['id'], $fileInfo["name"]);
                $outputStateConfiguration[] = [
                    'tags' => $file->getTags(),
                    'lastImportId' => $fileInfo['id']
                ];
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
