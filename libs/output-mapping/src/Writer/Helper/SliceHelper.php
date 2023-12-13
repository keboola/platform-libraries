<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\OutputMapping\Configuration\Table\Manifest;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\SliceSkippedException;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use Psr\Log\LoggerInterface;
use SplFileInfo as NativeSplFileInfo;
use Symfony\Component\Filesystem\Exception\FileNotFoundException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;
use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;

class SliceHelper
{
    public function __construct(
        private readonly LoggerInterface $logger,
        private readonly ?string $inputSizeThreshold = null,
    ) {
    }

    private function validateMappingSourcesAreUnique(array $mappingSources): void
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

    private function validateMappingSource(MappingSource $source): void
    {
        $sourceFile = $source->getSource();
        if (!$sourceFile instanceof LocalFileSource) {
            throw new SliceSkippedException('Only local files are supported for slicing.');
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
            $hasNonDefaultDelimiter = isset($mapping['delimiter'])
                && $mapping['delimiter'] !== Manifest::DEFAULT_DELIMITER;
            $hasNonDefaultEnclosure = isset($mapping['enclosure'])
                && $mapping['enclosure'] !== Manifest::DEFAULT_ENCLOSURE;
            $hasColumns = isset($mapping['columns']) && $mapping['columns'] !== [];

            if ($hasNonDefaultDelimiter || $hasNonDefaultEnclosure || $hasColumns) {
                throw new InvalidOutputException(
                    'Params "delimiter", "enclosure" or "columns" specified in mapping are not longer supported.',
                );
            }
        }
    }

    /**
     * @param MappingSource[] $mappingSources
     */
    public function sliceSources(array $mappingSources): array
    {
        $this->validateMappingSourcesAreUnique($mappingSources);

        foreach ($mappingSources as $i => $source) {
            try {
                $mappingSources[$i] = $this->sliceFile($source);
            } catch (SliceSkippedException $e) {
                $this->logger->warning(sprintf(
                    'Source "%s" slicing skipped: %s',
                    $source->getSourceName(),
                    $e->getMessage(),
                ));
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

    public function sliceFile(MappingSource $source): MappingSource
    {
        $this->validateMappingSource($source);

        /** @var LocalFileSource $sourceFile */
        $sourceFile = $source->getSource();
        $outputDirPath = uniqid($sourceFile->getFile()->getPathname() . '-', true);

        $outputDir = new NativeSplFileInfo($outputDirPath);

        $process = SliceCommandBuilder::createProcess(
            $source->getSourceName(),
            $sourceFile->getFile(),
            $outputDir,
            $source->getManifestFile(),
            $this->inputSizeThreshold,
        );

        $result = $process->run(function ($type, $buffer) {
            if ($type === Process::OUT) {
                $this->logger->info(trim($buffer));
            }
        });

        if ($result === SliceCommandBuilder::SLICER_SKIPPED_EXIT_CODE) {
            throw new SliceSkippedException('No need to slice, the source data is not large enough.');
        }

        if ($result !== 0) {
            throw new ProcessFailedException($process);
        }

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
            $this->getFile($sourceFile->getFile()->getPathname() . '.manifest'),
            $source->getMapping(),
        );
    }

    public function getFile(string $path): SplFileInfo
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
