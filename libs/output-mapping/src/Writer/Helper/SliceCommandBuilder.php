<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use SplFileInfo;

class SliceCommandBuilder
{
    public static function create(
        string $sourceName,
        SplFileInfo $inputFile,
        SplFileInfo $outputDir,
        ?SplFileInfo $inputManifestFile = null,
    ): array {
        $command = [
            './bin/slicer',
            '--table-input-path=' . $inputFile->getPathname(),
            '--table-name=' . $sourceName,
            '--table-output-path=' . $outputDir->getPathname(),
            '--table-output-manifest-path=' . $outputDir->getPathname() . '.manifest',
            '--gzip=false', // @TODO https://keboola.atlassian.net/browse/GCP-457
        ];

        if ($inputManifestFile && $inputManifestFile->isFile()) {
            // @TODO remove after new slicer is released https://github.com/keboola/processor-split-table/pull/25
            $command[] = '--table-input-manifest-path=' . $inputManifestFile->getPathname();
        }

        return $command;
    }
}
