<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use SplFileInfo;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;

class SliceHelper
{
    public static function sliceFile(MappingSource $source): MappingSource
    {
        //@TODO log process
        $sourceFile = $source->getSource();
        if (!$sourceFile instanceof LocalFileSource) {
            throw new InvalidArgumentException('Only LocalFileSource is supported.');
        }

        if ($sourceFile->isSliced()) {
            throw new InvalidArgumentException(sprintf('Sliced files are not yet supported.'));
        }

        if (!$sourceFile->getFile()->getSize()) {
            throw new InvalidArgumentException(sprintf('Empty files cannot be sliced.'));
        }

        $outputDirPath = uniqid($sourceFile->getFile()->getPathname() . '-', true);

        $ouputDir = new SplFileInfo($outputDirPath);

        $command = SliceCommandBuilder::create(
            $sourceFile->getFile(),
            $ouputDir,
            $source->getManifestFile(),
        );

        $process = new Process($command);
        $process->mustRun();

        $filesystem = new Filesystem();
        $filesystem->remove([$sourceFile->getFile()]);
        $filesystem->rename($ouputDir->getPathname(), $sourceFile->getFile()->getPathname());
        $filesystem->rename(
            $ouputDir->getPathname() . '.manifest',
            $sourceFile->getFile()->getPathname() . '.manifest',
            true,
        );

        return new MappingSource(
            $source->getSource(),
            FilesHelper::getFile($sourceFile->getFile()->getPathname() . '.manifest'),
            $source->getMapping(),
        );
    }
}
