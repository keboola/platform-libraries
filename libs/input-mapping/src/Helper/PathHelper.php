<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Helper;

use Keboola\InputMapping\Table\Options\InputTableOptions;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;

class PathHelper
{
    public static function ensurePathDelimiter(string $path): string
    {
        return self::ensureNoPathDelimiter($path) . '/';
    }

    public static function ensureNoPathDelimiter(string $path): string
    {
        return rtrim($path, '\\/');
    }

    public static function getManifestPath(
        FileStagingInterface $metadataStorage,
        string $destination,
        InputTableOptions $table,
    ): string {
        return self::ensurePathDelimiter($metadataStorage->getPath()) .
            self::getDestinationFilePath($destination, $table) . '.manifest';
    }

    public static function getDataFilePath(
        FileStagingInterface $dataStorage,
        string $destination,
        InputTableOptions $table,
    ): string {
        return self::ensurePathDelimiter($dataStorage->getPath()) .
            self::getDestinationFilePath($destination, $table);
    }

    private static function getDestinationFilePath(string $destination, InputTableOptions $table): string
    {
        if (!$table->getDestination()) {
            return $destination . '/' . $table->getSource();
        } else {
            return $destination . '/' . $table->getDestination();
        }
    }
}
