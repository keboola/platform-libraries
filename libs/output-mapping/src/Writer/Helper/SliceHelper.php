<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Exception\SliceSkippedException;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use SplFileInfo;
use Symfony\Component\Filesystem\Filesystem;

class SliceHelper
{
    /**
     * @param MappingSource[] $mappingSources
     */
    public static function sliceSources(array $mappingSources): array
    {
        $mappingSourceOccurrences = [];
        foreach ($mappingSources as $source) {
            $sourceName = $source->getSourceName();
            $mappingSourceOccurrences[$sourceName] = ($mappingSourceOccurrences[$sourceName] ?? 0) + 1;
        }

        foreach ($mappingSources as $i => $source) {
            if ($mappingSourceOccurrences[$source->getSourceName()] > 1) {
                continue; // @TODO log, alternatively implement
            }

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

        $outputDirPath = uniqid($sourceFile->getFile()->getPathname() . '-', true);

        $outputDir = new SplFileInfo($outputDirPath);

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
            FilesHelper::getFile($sourceFile->getFile()->getPathname() . '.manifest'),
            $source->getMapping(),
        );
    }
}
