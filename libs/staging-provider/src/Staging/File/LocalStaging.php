<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\File;

readonly class LocalStaging implements FileStagingInterface
{
    public function __construct(
        private string $path,
    ) {
    }

    public function getPath(): string
    {
        return $this->path;
    }
}
