<?php

declare(strict_types=1);

namespace Keboola\InputMapping\File;

interface StrategyInterface
{
    public function downloadFile(array $fileInfo, string $destinationPath, bool $overwrite): void;
}
