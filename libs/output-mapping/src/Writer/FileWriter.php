<?php

namespace Keboola\OutputMapping\Writer;

use Keboola\InputMapping\Reader;
use Keboola\OutputMapping\Configuration\File\Manifest as FileManifest;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\TagsRewriter;
use Keboola\StorageApi\ClientException;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class FileWriter extends AbstractWriter
{
    /**
     * Upload files from local temp directory to Storage.
     *
     * @param string $source Source path.
     * @param array $configuration Upload configuration
     * @param string $storage Currently any storage that is not ABS workspaces defaults to local
     */
    public function uploadFiles($source, $configuration, $storage)
    {
        $strategy = $this->strategyFactory->getFileOutputStrategy($storage);
        $files = $strategy->listFiles($source);
        $manifests = $strategy->listManifests($source);

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
            $fileNames[] = $file->getName();
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
        foreach ($manifests as $manifest) {
            if (!in_array(substr(basename($manifest->getName()), 0, -9), $fileNames)) {
                throw new InvalidOutputException('Found orphaned file manifest: \'' . basename($manifest->getName()) . "'");
            }
        }

        foreach ($files as $file) {
            $configFromMapping = [];
            $configFromManifest = [];
            if (isset($configuration['mapping'])) {
                foreach ($configuration['mapping'] as $mapping) {
                    if (isset($mapping['source']) && $mapping['source'] === $file->getName()) {
                        $configFromMapping = $mapping;
                        $processedOutputMappingFiles[] = $configFromMapping['source'];
                        unset($configFromMapping['source']);
                    }
                }
            }
            $manifestKey = $file->getPathName() . '.manifest';
            if (isset($manifests[$manifestKey])) {
                $configFromManifest = $strategy->readFileManifest($file->getPathName() . '.manifest');
                unset($manifests[$manifestKey]);
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
                    "Failed to write manifest for table {$file->getPathName()}.",
                    0,
                    $e
                );
            }
            try {
                $storageConfig = TagsRewriter::rewriteTags($storageConfig, $this->clientWrapper);
                $strategy->loadFileToStorage($file->getPathName(), $storageConfig);
            } catch (ClientException $e) {
                throw new InvalidOutputException(
                    "Cannot upload file '{$file->getName()}' to Storage API: " . $e->getMessage(),
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

    /**
     * Add tags to processed input files.
     * @param $configuration array
     */
    public function tagFiles(array $configuration)
    {
        foreach ($configuration as $fileConfiguration) {
            if (!empty($fileConfiguration['processed_tags'])) {
                $files = Reader::getFiles($fileConfiguration, $this->clientWrapper, $this->logger);
                foreach ($files as $file) {
                    foreach ($fileConfiguration['processed_tags'] as $tag) {
                        $this->clientWrapper->getBasicClient()->addFileTag($file['id'], $tag);
                    }
                }
            }
        }
    }
}
