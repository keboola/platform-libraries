<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\FileStorage\Abs\ClientFactory;
use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\WorkspaceProviderInterface;
use Keboola\OutputMapping\Configuration\File\Manifest as FileManifest;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\ManifestHelper;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Blob\Models\ListBlobsOptions;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

class AbsWorkspaceFileWriter extends AbstractWriter
{
    /**
     * @var WorkspaceProviderInterface
     */
    private $workspaceProvider;

    /**
     * @var BlobRestProxy
     */
    private $blobClient;

    /**
     * @var string workspace container name
     */
    private $container;

    /**
     * AbstractWriter constructor.
     *
     * @param ClientWrapper $clientWrapper
     * @param LoggerInterface $logger
     * @param WorkspaceProviderInterface $workspaceProvider
     */
    public function __construct(ClientWrapper $clientWrapper, LoggerInterface $logger, WorkspaceProviderInterface $workspaceProvider = null)
    {
        parent::__construct($clientWrapper, $logger);
        $this->workspaceProvider = $workspaceProvider ? $workspaceProvider : new NullWorkspaceProvider();
        $credentials = $this->workspaceProvider->getCredentials(WorkspaceProviderInterface::TYPE_ABS);
        $this->blobClient = ClientFactory::createClientFromConnectionString($credentials['connectionString']);
        $this->container = $credentials['container'];
    }

    public function uploadFiles($source, $configuration = [])
    {
        $manifestNames = ManifestHelper::getManifestFiles($source);

        $listBlobOptions = new ListBlobsOptions();
        $listBlobOptions->setP();

        $files = $this->blobClient->listBlobs($this->container, );

        $finder = new Finder();
        /** @var SplFileInfo[] $files */
        $files = $finder->files()->notName('*.manifest')->in($source)->depth(0);

        $outputMappingFiles = [];
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                $outputMappingFiles[] = $mapping['source'];
            }
        }
        $outputMappingFiles = array_unique($outputMappingFiles);
        $processedOutputMappingFiles = [];

        $fileNames = [];
        foreach ($files as $file) {
            $fileNames[] = $file->getFilename();
        }

        // Check if all files from output mappings are present
        if (isset($configuration['mapping'])) {
            foreach ($configuration['mapping'] as $mapping) {
                if (!in_array($mapping['source'], $fileNames)) {
                    throw new InvalidOutputException("File '{$mapping["source"]}' not found.", 404);
                }
            }
        }

        // Check for manifest orphans
        foreach ($manifestNames as $manifest) {
            if (!in_array(substr(basename($manifest), 0, -9), $fileNames)) {
                throw new InvalidOutputException('Found orphaned file manifest: \'' . basename($manifest) . "'");
            }
        }

        foreach ($files as $file) {
            $configFromMapping = [];
            $configFromManifest = [];
            if (isset($configuration['mapping'])) {
                foreach ($configuration['mapping'] as $mapping) {
                    if (isset($mapping['source']) && $mapping['source'] === $file->getFilename()) {
                        $configFromMapping = $mapping;
                        $processedOutputMappingFiles[] = $configFromMapping['source'];
                        unset($configFromMapping['source']);
                    }
                }
            }
            $manifestKey = array_search($file->getPathname() . '.manifest', $manifestNames);
            if ($manifestKey !== false) {
                $configFromManifest = $this->readFileManifest($file->getPathname() . '.manifest');
                unset($manifestNames[$manifestKey]);
            }
            try {
                // Mapping with higher priority
                if ($configFromMapping || !$configFromManifest) {
                    $storageConfig = (new FileManifest())->parse([$configFromMapping]);
                } else {
                    $storageConfig = (new FileManifest())->parse([$configFromManifest]);
                }
            } catch (InvalidConfigurationException $e) {
                throw new InvalidOutputException(
                    "Failed to write manifest for table {$file->getFilename()}.",
                    0,
                    $e
                );
            }
            try {
                $this->uploadFile($file->getPathname(), $storageConfig);
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    "Cannot upload file '{$file->getFilename()}' to Storage API: " . $e->getMessage(),
                    $e->getCode(),
                    $e
                );
            }
        }

        $processedOutputMappingFiles = array_unique($processedOutputMappingFiles);
        $diff = array_diff(
            array_merge($outputMappingFiles, $processedOutputMappingFiles),
            $processedOutputMappingFiles
        );
        if (count($diff)) {
            throw new InvalidOutputException(
                "Couldn't process output mapping for file(s) '" . join("', '", $diff) . "'."
            );
        }
    }
}
