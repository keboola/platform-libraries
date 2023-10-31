<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use Symfony\Component\Process\Process;

class SliceHelper
{
    public static function sliceFile(MappingSource $source): void
    {
        $sourceFile = $source->getSource();
        if (!$sourceFile instanceof LocalFileSource) {
            throw new InvalidArgumentException('Only LocalFileSource is supported.');
        }

        if ($sourceFile->isSliced()) {
            throw new InvalidArgumentException(sprintf('Sliced files are not yet supported.'));
        }

        $command = SliceCommandBuilder::create(
            $sourceFile->getFile(),
            $source->getManifestFile(),
        );

        //@TODO return new mapping source
        //@TODO log ouptut
        $process = new Process($command);
        $process->mustRun();
    }
}
