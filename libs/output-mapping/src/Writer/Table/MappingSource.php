<?php

namespace Keboola\OutputMapping\Writer\Table;

use InvalidArgumentException;
use SplFileInfo;

class MappingSource
{
    /** @var string */
    private $name;

    /** @var string */
    private $id;

    /** @var null|SplFileInfo */
    private $manifestFile;

    /** @var null|array */
    private $mapping;

    /**
     * @param string $sourceName
     * @param string $sourceId
     */
    public function __construct($sourceName, $sourceId, SplFileInfo $manifestFile = null, array $mapping = null)
    {
        $this->name = $sourceName;
        $this->id = $sourceId;
        $this->manifestFile = $manifestFile;
        $this->mapping = $mapping;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @return string
     */
    public function getId()
    {
        return $this->id;
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
