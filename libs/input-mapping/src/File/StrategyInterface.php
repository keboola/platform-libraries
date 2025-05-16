<?php

declare(strict_types=1);

namespace Keboola\InputMapping\File;

use Keboola\InputMapping\Configuration\Adapter;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

interface StrategyInterface
{
    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        FileStagingInterface $dataStorage,
        FileStagingInterface $metadataStorage,
        InputFileStateList $fileStateList,
        string $format = Adapter::FORMAT_JSON,
    );

    public function downloadFile(
        array $fileInfo,
        string $sourceBranchId,
        string $destinationPath,
        bool $overwrite,
    ): void;

    public function downloadFiles(array $fileConfigurations, string $destination): InputFileStateList;
}
