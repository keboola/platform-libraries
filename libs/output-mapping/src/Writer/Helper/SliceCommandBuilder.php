<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use SplFileInfo;

class SliceCommandBuilder
{
    public static function create(SplFileInfo $souceFile, ?SplFileInfo $manifestFile = null): array
    {
        $command = [
            './bin/slicer',
            '--table-input-path=' . $souceFile->getPathname(),
            '--table-name=LOG_PLACEHOLDER', // @TODO optional? we do not know at this moment the table name
            '--table-output-path=' . $souceFile->getPathname() . 'sliced',
            '--table-output-manifest-path=' . $souceFile->getPathname() . 'sliced.manifest',
            '--gzip=false', // for tests purpose
        ];

        if ($manifestFile && $manifestFile->isFile()) {
            // @TODO if manifest does not exists, the slicer does not fail
            $command[] = '--table-input-manifest-path=' . $manifestFile->getPathname();
        }

        return $command;
    }
}
