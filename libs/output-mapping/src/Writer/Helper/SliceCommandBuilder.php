<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Helper;

use Keboola\Slicer\Slicer;
use Symfony\Component\Process\Process;

class SliceCommandBuilder
{
    public const SLICER_SKIPPED_EXIT_CODE = 200;

    public static function createProcess(
        string $sourceName,
        string $inputPath,
        string $outputPath,
        string $dataType,
        ?string $inputSizeThreshold = null,
    ): Process {
        $command = [
            Slicer::getBinaryPath(),
            '--table-input-path=' . $inputPath,
            '--table-name=' . $sourceName,
            '--table-output-path=' . $outputPath,
            '--data-type-mode=' . self::resolveDataType($dataType),
            '--table-output-manifest-path=' . $outputPath . '.manifest',
            '--gzip=true',
            '--input-size-low-exit-code=' . self::SLICER_SKIPPED_EXIT_CODE,
        ];

        $manifestFile = $inputPath . '.manifest';
        if (file_exists($manifestFile)) {
            $command[] = '--table-input-manifest-path=' . $manifestFile;
        }

        if ($inputSizeThreshold) {
            $command[] = '--input-size-threshold=' . $inputSizeThreshold;
        }

        return new Process(
            command: $command,
            timeout: 7200.0,
        );
    }

    private static function resolveDataType(string $dataType): string
    {
        return match ($dataType) {
            'none' => 'columns',
            default => 'schema',
        };
    }
}
