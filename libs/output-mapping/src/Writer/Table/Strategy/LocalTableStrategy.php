<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use InvalidArgumentException;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\OutputMapping\Writer\Helper\Path;
use Keboola\OutputMapping\Writer\Helper\SliceHelper;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\StorageApi\Options\FileUploadOptions;
use SplFileInfo;
use Symfony\Component\Finder\Finder;

class LocalTableStrategy extends AbstractTableStrategy
{
    /**
     * @return MappingSource[]
     */
    public function resolveMappingSources(string $sourcePathPrefix, array $configuration): array
    {
        $sourcesPath = Path::join($this->metadataStorage->getPath(), $sourcePathPrefix);
        $dataFiles = FilesHelper::getDataFiles($sourcesPath);
        $manifestFiles = FilesHelper::getManifestFiles($sourcesPath);

        /** @var array<string, MappingSource> $mappingSources */
        $mappingSources = [];

        foreach ($dataFiles as $file) {
            $sourceName = $file->getBasename();
            $mappingSources[$sourceName] = new MappingSource(new LocalFileSource($file));
        }

        foreach ($manifestFiles as $file) {
            $sourceName = $file->getBasename('.manifest');

            if (!isset($mappingSources[$sourceName])) {
                throw new InvalidOutputException(sprintf('Found orphaned table manifest: "%s"', $file->getBasename()));
            }

            $mappingSources[$sourceName]->setManifestFile($file);
        }

        return $this->combineSourcesWithMappingsFromConfiguration(
            $this->sliceSources($mappingSources), //@TODO only for feature or BQ or large files
            $configuration['mapping'] ?? [],
        );
    }

    /**
     * @param MappingSource[] $mappingSources
     */
    private function sliceSources(array $mappingSources): array
    {
        foreach ($mappingSources as $sourceName => $source) {
            try {
                $mappingSources[$sourceName] = SliceHelper::sliceFile($source);
            } catch (InvalidArgumentException $e) {
                // invalid inputs should not fail the OM process
            }
        }

        return $mappingSources;
    }

    public function prepareLoadTaskOptions(SourceInterface $source, array $config): array
    {
        if (!$source instanceof LocalFileSource) {
            throw new InvalidArgumentException(sprintf(
                'Argument $source is expected to be instance of %s, %s given',
                LocalFileSource::class,
                get_class($source),
            ));
        }

        $loadOptions = [
            'delimiter' => $config['delimiter'],
            'enclosure' => $config['enclosure'],
        ];

        $tags = !empty($config['tags']) ? $config['tags'] : [];

        if ($source->isSliced()) {
            $loadOptions['dataFileId'] = $this->uploadSlicedFile($source->getFile(), $tags);
        } else {
            $loadOptions['dataFileId'] = $this->uploadRegularFile($source->getFile(), $tags);
        }

        return $loadOptions;
    }

    private function uploadSlicedFile(SplFileInfo $source, array $tags): string
    {
        $finder = new Finder();
        $slices = $finder->files()->in($source->getPathname())->depth(0);
        $sliceFiles = [];
        foreach ($slices as $slice) {
            $sliceFiles[] = $slice->getPathname();
        }

        $fileUploadOptions = (new FileUploadOptions())
            ->setIsSliced(true)
            ->setFileName($source->getBasename())
            ->setCompress(true)
            ->setTags($tags)
        ;

        return (string) $this->clientWrapper->getTableAndFileStorageClient()->uploadSlicedFile(
            $sliceFiles,
            $fileUploadOptions,
        );
    }

    private function uploadRegularFile(SplFileInfo $source, array $tags): string
    {
        $fileUploadOptions = (new FileUploadOptions())
            ->setCompress(true)
            ->setTags($tags)
        ;

        return (string) $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
            $source->getPathname(),
            $fileUploadOptions,
        );
    }
}
