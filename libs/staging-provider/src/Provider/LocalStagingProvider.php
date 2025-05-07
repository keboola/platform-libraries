<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Provider;

use Keboola\InputMapping\Staging\FileStagingInterface;

readonly class LocalStagingProvider implements FileStagingInterface
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
