<?php

declare(strict_types=1);

namespace Keboola\InputMapping\File;

use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StorageApi\Client;

interface StrategyInterface
{
    public function downloadFile(
        array $fileInfo,
        string $sourceBranchId,
        string $destinationPath,
        bool $overwrite,
    ): void;

    public function downloadFiles(array $fileConfigurations, string $destination): InputFileStateList;
}
