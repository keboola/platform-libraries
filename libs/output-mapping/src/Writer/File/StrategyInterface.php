<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\File;

use Keboola\OutputMapping\Configuration\Adapter;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

interface StrategyInterface
{
    /**
     * @param Adapter::FORMAT_* $format
     */
    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        FileStagingInterface $dataStorage,
        FileStagingInterface $metadataStorage,
        string $format,
    );

    /**
     * @return FileItem[]
     */
    public function listFiles(string $dir): array;

    /**
     * @return FileItem[] Indexed by file path.
     */
    public function listManifests(string $dir): array;

    /**
     * @param string $file - fully qualified path to file
     * @param array $storageConfig
     * @return string Storage File Id
     */
    public function loadFileToStorage(string $file, array $storageConfig): string;

    public function readFileManifest(string $manifestFile): array;
}
