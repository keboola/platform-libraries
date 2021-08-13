<?php

namespace Keboola\OutputMapping\Writer\Table\Source;

use SplFileInfo;

class TableSource implements SourceInterface
{
    /** @var string */
    private $table;

    /** @var null|SplFileInfo */
    private $manifestFile;

    /** @var null|array */
    private $mapping;

    public function __construct($table, SplFileInfo $manifestFile = null, array $mapping = null)
    {
        $this->table = $table;
        $this->manifestFile = $manifestFile;
        $this->mapping = $mapping;
    }

    public function getSourceName()
    {
        return $this->table;
    }

    public function getSourcePath()
    {
        return $this->table;
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
