<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Writer\SourceInterface;
use Symfony\Component\Finder\SplFileInfo;

class MappingSource
{
    public function __construct(
        private readonly SourceInterface $source,
        private ?SplFileInfo $manifestFile = null,
        private ?array $mapping = null,
    ) {
    }

    public function getSource(): SourceInterface
    {
        return $this->source;
    }

    public function getSourceName(): string
    {
        return $this->source->getName();
    }

    public function setManifestFile(?SplFileInfo $file): void
    {
        $this->manifestFile = $file;
    }

    public function getManifestFile(): ?SplFileInfo
    {
        return $this->manifestFile;
    }

    public function setMapping(?array $mapping): void
    {
        $this->mapping = $mapping;
    }

    public function getMapping(): ?array
    {
        return $this->mapping;
    }
}
