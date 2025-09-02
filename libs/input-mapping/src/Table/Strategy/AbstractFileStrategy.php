<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Table\Strategy;

use InvalidArgumentException;
use Keboola\InputMapping\Helper\ManifestCreator;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

abstract class AbstractFileStrategy extends AbstractStrategy
{
    protected readonly FileStagingInterface $dataStorage;
    protected readonly ManifestCreator $manifestCreator;

    public function __construct(
        protected readonly ClientWrapper $clientWrapper,
        protected readonly LoggerInterface $logger,
        StagingInterface $dataStorage,
        protected readonly FileStagingInterface $metadataStorage,
        protected readonly InputTableStateList $tablesState,
        protected readonly string $destination,
        protected readonly FileFormat $format,
    ) {
        if (!$dataStorage instanceof FileStagingInterface) {
            throw new InvalidArgumentException('Data storage must be instance of FileStagingInterface');
        }

        $this->dataStorage = $dataStorage;
        $this->manifestCreator = new ManifestCreator();
    }

    protected function getMetadataStorage(): FileStagingInterface
    {
        return $this->metadataStorage;
    }

    protected function getDestination(): string
    {
        return $this->destination;
    }
}
