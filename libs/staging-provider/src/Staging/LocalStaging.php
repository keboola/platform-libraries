<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging;

class LocalStaging implements StagingInterface
{
    public function __construct(private readonly string $path)
    {
    }

    public static function getType(): string
    {
        return 'local';
    }

    public function getPath(): string
    {
        return $this->path;
    }
}
