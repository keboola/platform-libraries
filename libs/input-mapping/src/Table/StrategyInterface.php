<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table;

use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

interface StrategyInterface
{
    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        StagingInterface $dataStorage,
        FileStagingInterface $metadataStorage,
        InputTableStateList $tablesState,
        string $destination,
        FileFormat $format,
    );

    public function downloadTable(RewrittenInputTableOptions $table): array;

    public function handleExports(array $exports, bool $preserve): array;
}
