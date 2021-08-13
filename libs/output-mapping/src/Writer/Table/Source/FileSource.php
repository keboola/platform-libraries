<?php

namespace Keboola\OutputMapping\Writer\Table\Source;

use SplFileInfo;

class FileSource implements SourceInterface
{
    /** @var SplFileInfo */
    private $dataFile;

    /** @var null|SplFileInfo */
    private $manifestFile;

    /** @var null|array */
    private $mapping;

    public function __construct(SplFileInfo $dataFile, SplFileInfo $manifestFile = null, array $mapping = null)
    {
        $this->dataFile = $dataFile;
        $this->manifestFile = $manifestFile;
        $this->mapping = $mapping;
    }

    public function getSourceName()
    {
        return $this->dataFile->getBasename();
    }

    public function getSourcePath()
    {
        return $this->dataFile->getPathname();
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
