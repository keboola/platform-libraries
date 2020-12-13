<?php

namespace Keboola\OutputMapping\Writer;

use Exception;
use Keboola\InputMapping\Reader;
use Keboola\OutputMapping\Configuration\File\Manifest as FileManifest;
use Keboola\OutputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\ManifestHelper;
use Keboola\OutputMapping\Writer\Helper\TagsRewriter;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

class FileWriter extends AbstractWriter
{
    /**
     * Upload files from local temp directory to Storage.
     *
     * @param string $source Source path.
     * @param array $configuration Upload configuration
     */
    public function uploadFiles($source, $configuration = [])
    {

        $manifestNames = ManifestHelper::getManifestFiles($source);

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
                $storageConfig = TagsRewriter::rewriteTags($storageConfig, $this->clientWrapper);
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

    /**
     * @param $source
     * @return array
     */
    private function readFileManifest($source)
    {
        $adapter = new FileAdapter($this->format);
        try {
            return $adapter->readFromFile($source);
        } catch (Exception $e) {
            throw new InvalidOutputException(
                sprintf('Failed to parse manifest file "%s" as "%s": %s', $source, $this->format, $e->getMessage()),
                $e->getCode(),
                $e
            );
        }
    }

    /**
     * @param $source
     * @param array $config
     * @throws ClientException
     */
    private function uploadFile($source, array $config = [])
    {
        $options = new FileUploadOptions();
        $options
            ->setTags(array_unique($config['tags']))
            ->setIsPermanent($config['is_permanent'])
            ->setIsEncrypted($config['is_encrypted'])
            ->setIsPublic($config['is_public'])
            ->setNotify($config['notify']);
        $this->clientWrapper->getBasicClient()->uploadFile($source, $options);
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
