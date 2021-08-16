<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer\Table\Source;

use SplFileInfo;

abstract class AbstractSource implements SourceInterface
{
    /** @var string */
    private $sourcePathPrefix;

    /** @var null|SplFileInfo */
    private $manifestFile;

    /** @var null|array */
    private $mapping;

    /**
     * @param string $sourcePathPrefix
     */
    public function __construct($sourcePathPrefix, SplFileInfo $manifestFile = null, array $mapping = null)
    {
        $this->sourcePathPrefix = $sourcePathPrefix;
        $this->manifestFile = $manifestFile;
        $this->mapping = $mapping;
    }

    public function getSourcePathPrefix()
    {
        return $this->sourcePathPrefix;
    }

    public function setManifestFile(SplFileInfo $file = null)
    {
        $this->manifestFile = $file;
    }

    public function getManifestFile()
    {
        return $this->manifestFile;
    }

    public function setMapping(array $mapping = null)
    {
        $this->mapping = $mapping;
    }

    public function getMapping()
    {
        return $this->mapping;
    }
}
