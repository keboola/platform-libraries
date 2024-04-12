<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;

class MappingFromRawConfigurationAndPhysicalData
{
    private SourceInterface $dataItem;
    private ?MappingFromRawConfiguration $mappingItem;

    public function __construct(SourceInterface $dataItem, ?MappingFromRawConfiguration $mappingItem)
    {
        $this->dataItem = $dataItem;
        $this->mappingItem = $mappingItem;
    }

    public function getSourceName(): string
    {
        return $this->dataItem->getName();
    }

    public function getManifestName(): string
    {
        return $this->dataItem->getName() . '.manifest';
    }

    public function getConfiguration(): ?MappingFromRawConfiguration
    {
        return $this->mappingItem;
    }

    public function isSliced(): bool
    {
        return $this->dataItem->isSliced();
    }

    public function getPathName(): string
    {
        if (method_exists($this->dataItem, 'getPathName') === false) {
            throw new InvalidOutputException('PathName is available only for FileItem');
        }
        return $this->dataItem->getPathName();
    }

    public function getPath(): string
    {
        if (method_exists($this->dataItem, 'getPath') === false) {
            throw new InvalidOutputException('Path is available only for FileItem');
        }
        return $this->dataItem->getPath();
    }

    /**
     * @return class-string<SourceInterface>
     */
    public function getItemSourceClass(): string
    {
        return get_class($this->dataItem);
    }

    public function getWorkspaceId(): string
    {
        if ($this->getItemSourceClass() !== WorkspaceItemSource::class) {
            throw new InvalidOutputException('WorkspaceId is available only for WorkspaceItemSource');
        }
        return $this->dataItem->getWorkspaceId();
    }

    public function getDataObject(): string
    {
        if ($this->getItemSourceClass() !== WorkspaceItemSource::class) {
            throw new InvalidOutputException('DataObject is available only for WorkspaceItemSource');
        }
        return $this->dataItem->getDataObject();
    }
}
