<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use SplFileInfo;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class SliceHelper
{
    public static function sliceFile(MappingSource $source): void
    {
        //@TODO log process
        $sourceFile = $source->getSource();
        if (!$sourceFile instanceof LocalFileSource) {
            throw new InvalidArgumentException('Only LocalFileSource is supported.');
        }

        if ($sourceFile->isSliced()) {
            throw new InvalidArgumentException(sprintf('Sliced files are not yet supported.'));
        }

        $outputDirPath = uniqid($sourceFile->getFile()->getPathname() . '-', true);

        $ouputDir = new SplFileInfo($outputDirPath);
        $slicedManfest = new SplFileInfo($sourceFile->getFile()->getPathname() . 'sliced.manifest');

        $command = SliceCommandBuilder::create(
            $sourceFile->getFile(),
            $ouputDir,
            $slicedManfest,
        );

        $process = new Process($command);
        $process->mustRun();

        $filesystem = new Filesystem();
        $filesystem->remove([$sourceFile->getFile()]);
        $filesystem->rename($ouputDir->getPathname(), $sourceFile->getFile()->getPathname());
        $filesystem->rename(
            $ouputDir->getPathname() . '.manifest',
            $sourceFile->getFile()->getPathname() . '.manifest',
        );
    }
}
