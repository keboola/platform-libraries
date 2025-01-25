<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\Source\SourceType;

class MappingFromRawConfigurationAndPhysicalDataWithManifest
{
    public function __construct(
        private readonly MappingFromRawConfigurationAndPhysicalData $source,
        private readonly ?FileItem $manifest,
    ) {
    }

    public function getSourceName(): string
    {
        return $this->source->getSourceName();
    }

    public function getPathNameManifest(): string
    {
        return $this->getPathName() . '.manifest';
    }

    public function getManifest(): ?FileItem
    {
        return $this->manifest;
    }

    public function getConfiguration(): ?MappingFromRawConfiguration
    {
        return $this->source->getConfiguration();
    }

    public function isSliced(): bool
    {
        return $this->source->isSliced();
    }

    public function getPathName(): string
    {
        return $this->source->getPathName();
    }

    public function getPath(): string
    {
        return $this->source->getPath();
    }

    public function getWorkspaceId(): string
    {
        return $this->source->getWorkspaceId();
    }

    public function getDataObject(): string
    {
        return $this->source->getDataObject();
    }

    public function getSourceType(): SourceType
    {
        return $this->source->getItemSourceType();
    }
}
