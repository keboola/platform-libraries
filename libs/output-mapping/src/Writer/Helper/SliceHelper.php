<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\SliceSkippedException;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use SplFileInfo as NativeSplFileInfo;
use Symfony\Component\Filesystem\Exception\FileNotFoundException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;

class SliceHelper
{
    private static function validateMappingSourcesAreUnique(array $mappingSources): void
    {
        $mappingSourceOccurrences = [];
        foreach ($mappingSources as $source) {
            $sourceName = $source->getSourceName();
            $mappingSourceOccurrences[$sourceName] = ($mappingSourceOccurrences[$sourceName] ?? 0) + 1;
        }

        foreach ($mappingSources as $source) {
            if ($mappingSourceOccurrences[$source->getSourceName()] > 1) {
                throw new InvalidOutputException(sprintf(
                    'Source "%s" has multiple destinations set.',
                    $source->getSourceName(),
                ));
            }
        }
    }

    private static function validateMappingSource(MappingSource $source): void
    {
        $sourceFile = $source->getSource();
        if (!$sourceFile instanceof LocalFileSource) {
            throw new SliceSkippedException('Only local files is supported for slicing.');
        }

        if ($sourceFile->isSliced()) {
            throw new SliceSkippedException('Sliced files are not yet supported.');
        }

        if (!$sourceFile->getFile()->getSize()) {
            throw new SliceSkippedException(sprintf('Empty files cannot be sliced.'));
        }

        if ($source->getMapping()) {
            // @TODO remove after fix https://keboola.atlassian.net/browse/GCP-472
            $mapping = $source->getMapping();
            if (isset($mapping['delimiter']) || isset($mapping['enclosure'])) {
                throw new SliceSkippedException(
                    'Params "delimiter" or "enclosure" specified in mapping are not supported by slicer.',
                );
            }
            if (isset($mapping['columns'])) {
                throw new SliceSkippedException(
                    'Param "columns" specified in mapping is not supported by slicer.',
                );
            }
        }
    }

    /**
     * @param MappingSource[] $mappingSources
     */
    public static function sliceSources(array $mappingSources): array
    {
        self::validateMappingSourcesAreUnique($mappingSources);

        foreach ($mappingSources as $i => $source) {
            try {
                $mappingSources[$i] = self::sliceFile($source);
            } catch (SliceSkippedException $e) {
                // invalid inputs should not fail the OM process
                $mappingSources[$i] = new MappingSource(
                    clone $source->getSource(),
                    $source->getManifestFile() ? clone $source->getManifestFile() : null,
                    $source->getMapping(),
                );
            }
        }

        return $mappingSources;
    }

    public static function sliceFile(MappingSource $source): MappingSource
    {
        //@TODO log process
        self::validateMappingSource($source);

        /** @var LocalFileSource @$sourceFile */
        $sourceFile = $source->getSource();
        $outputDirPath = uniqid($sourceFile->getFile()->getPathname() . '-', true);

        $outputDir = new NativeSplFileInfo($outputDirPath);

        $process = SliceCommandBuilder::createProcess(
            $source->getSourceName(),
            $sourceFile->getFile(),
            $outputDir,
            $source->getManifestFile(),
        );
        $process->mustRun();

        $filesystem = new Filesystem();
        $filesystem->remove([$sourceFile->getFile()]);
        $filesystem->rename($outputDir->getPathname(), $sourceFile->getFile()->getPathname());
        $filesystem->rename(
            $outputDir->getPathname() . '.manifest',
            $sourceFile->getFile()->getPathname() . '.manifest',
            true,
        );

        return new MappingSource(
            clone $source->getSource(),
            self::getFile($sourceFile->getFile()->getPathname() . '.manifest'),
            $source->getMapping(),
        );
    }

    public static function getFile(string $path): SplFileInfo
    {
        $fileInfo = new NativeSplFileInfo($path);
        $files = (new Finder())->files()
            ->name($fileInfo->getFilename())
            ->in($fileInfo->getPath())
            ->depth(0);

        if (!$files->count()) {
            throw new FileNotFoundException(
                path: $path,
            );
        }

        $iterator = $files->getIterator();
        $iterator->rewind();

        return $iterator->current();
    }
}
