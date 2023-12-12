<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use SplFileInfo;
use Symfony\Component\Process\Process;

class SliceCommandBuilder
{
    public static function createProcess(
        string $sourceName,
        SplFileInfo $inputFile,
        SplFileInfo $outputDir,
        ?SplFileInfo $inputManifestFile = null,
        ?string $inputSizeThreshold = null,
    ): Process {
        $command = [
            './bin/slicer',
            '--table-input-path=' . $inputFile->getPathname(),
            '--table-name=' . $sourceName,
            '--table-output-path=' . $outputDir->getPathname(),
            '--table-output-manifest-path=' . $outputDir->getPathname() . '.manifest',
            '--gzip=true',
        ];

        if ($inputManifestFile) {
            $command[] = '--table-input-manifest-path=' . $inputManifestFile->getPathname();
        }

        if ($inputSizeThreshold) {
            $command[] = '--input-size-threshold=' . $inputSizeThreshold;
        }

        return new Process(
            command: $command,
            timeout: 7200.0,
        );
    }
}
