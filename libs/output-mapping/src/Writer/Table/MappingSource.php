<?php

namespace Keboola\OutputMapping\Writer\Table;

use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Symfony\Component\Finder\SplFileInfo;

class MappingSource
{
    /** @var SourceInterface */
    private $source;

    /** @var null|SplFileInfo */
    private $manifestFile;

    /** @var null|array */
    private $mapping;

    public function __construct(
        SourceInterface $source,
        SplFileInfo $manifestFile = null,
        array $mapping = null
    ) {
        $this->source = $source;
        $this->manifestFile = $manifestFile;
        $this->mapping = $mapping;
    }

    /**
     * @return SourceInterface
     */
    public function getSource()
    {
        return $this->source;
    }

    /**
     * @return string
     */
    public function getSourceName()
    {
        return $this->source->getName();
    }

    /**
     * @param SplFileInfo|null $file
     */
    public function setManifestFile($file)
    {
        $this->manifestFile = $file;
    }

    /**
     * @return SplFileInfo|null
     */
    public function getManifestFile()
    {
        return $this->manifestFile;
    }

    /**
     * @param array|null $mapping
     */
    public function setMapping($mapping)
    {
        $this->mapping = $mapping;
    }

    /**
     * @return array|null
     */
    public function getMapping()
    {
        return $this->mapping;
    }
}
