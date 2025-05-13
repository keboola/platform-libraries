<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\File;

use Keboola\StagingProvider\Exception\StagingProviderException;

class NullFileStaging implements FileStagingInterface
{
    public function getPath(): never
    {
        $this->throwError();
    }

    private function throwError(): never
    {
        throw new StagingProviderException('File staging is not available');
    }
}
