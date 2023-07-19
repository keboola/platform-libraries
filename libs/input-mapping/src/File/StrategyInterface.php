<?php

declare(strict_types=1);

namespace Keboola\InputMapping\File;

use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StorageApi\Client;

interface StrategyInterface
{
    public function downloadFile(array $fileInfo, string $destinationPath, bool $overwrite, Client $client): void;
    public function downloadFiles(array $fileConfigurations, string $destination): InputFileStateList;
}
