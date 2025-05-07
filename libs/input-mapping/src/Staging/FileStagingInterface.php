<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

interface FileStagingInterface extends StagingInterface
{
    public function getPath(): string;
}
