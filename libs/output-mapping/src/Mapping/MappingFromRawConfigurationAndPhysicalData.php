<?php

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Lister\PhysicalItem;
use Keboola\OutputMapping\Writer\FileItem;

class MappingFromRawConfigurationAndPhysicalData
{
    private FileItem $dataItem;
    private ?MappingFromRawConfiguration $mappingItem;

    public function __construct(FileItem $dataItem, ?MappingFromRawConfiguration $mappingItem)
    {
        $this->dataItem = $dataItem;
        $this->mappingItem = $mappingItem;
    }

    public function getSourceName(): string
    {
        return $this->dataItem->getName();
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
        return $this->dataItem->getPathName();
    }

    public function getPath(): string
    {
        return $this->dataItem->getPath();
    }

}
