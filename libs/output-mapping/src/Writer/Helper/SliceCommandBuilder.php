<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use SplFileInfo;

class SliceCommandBuilder
{
    public static function create(
        SplFileInfo $inputFile,
        SplFileInfo $outputDir,
        ?SplFileInfo $inputManifestFile = null,
    ): array {
        $command = [
            './bin/slicer',
            '--table-input-path=' . $inputFile->getPathname(),
            '--table-name=LOG_PLACEHOLDER', // @TODO optional? we do not know at this moment the table name
            '--table-output-path=' . $outputDir->getPathname(),
            '--table-output-manifest-path=' . $outputDir->getPathname() . '.manifest',
            '--gzip=false', // for tests purpose, gzip will be implemented in future
        ];

        if ($inputManifestFile && $inputManifestFile->isFile()) {
            // @TODO if manifest does not exists, the slicer does not fail
            $command[] = '--table-input-manifest-path=' . $inputManifestFile->getPathname();
        }

        return $command;
    }
}
