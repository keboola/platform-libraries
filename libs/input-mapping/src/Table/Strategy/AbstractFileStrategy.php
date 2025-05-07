<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use Keboola\InputMapping\Configuration\Adapter;
use Keboola\InputMapping\Staging\FileStagingInterface;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractFileStrategy extends AbstractStrategy
{
    /**
     * @param Adapter::FORMAT_* $format
     */
    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        protected readonly FileStagingInterface $dataStorage,
        protected readonly FileStagingInterface $metadataStorage,
        InputTableStateList $tablesState,
        string $destination,
        string $format = 'json',
    ) {
        parent::__construct(
            $clientWrapper,
            $logger,
            $tablesState,
            $destination,
            $format,
        );
    }
}
