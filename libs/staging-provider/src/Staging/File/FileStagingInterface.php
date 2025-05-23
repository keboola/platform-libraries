<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\File;

use Keboola\StagingProvider\Staging\StagingInterface;

interface FileStagingInterface extends StagingInterface
{
    public function getPath(): string;
}
