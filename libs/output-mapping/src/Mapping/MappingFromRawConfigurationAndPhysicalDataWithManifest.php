<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Mapping;

use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;

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
        return $this->getPath() . $this->source->getManifestName();
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

    /**
     * @return class-string<SourceInterface>
     */
    public function getItemSourceClass(): string
    {
        return $this->source->getItemSourceClass();
    }

    public function getWorkspaceId(): string
    {
        return $this->source->getWorkspaceId();
    }

    public function getDataObject(): string
    {
        return $this->source->getDataObject();
    }
}
