<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Strategy;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Table\MappingResolver\LocalMappingResolver;
use Keboola\OutputMapping\Writer\Table\MappingResolver\MappingResolverInterface;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\StorageApi\Options\FileUploadOptions;
use SplFileInfo;
use Symfony\Component\Finder\Finder;

class LocalTableStrategy extends AbstractTableStrategy
{
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

    public function getMappingResolver(): MappingResolverInterface
    {
        return new LocalMappingResolver($this->metadataStorage->getPath());
    }
}
