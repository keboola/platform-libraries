<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Staging\File;

enum FileFormat: string
{
    case Json = 'json';
    case Yaml = 'yaml';

    public function getFileExtension(): string
    {
        return match ($this) {
            self::Yaml => '.yml',
            self::Json => '.json',
        };
    }
}
