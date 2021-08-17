<?php

namespace Keboola\OutputMapping\Writer\Table;

use InvalidArgumentException;
use Symfony\Component\Finder\SplFileInfo;

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
        if (!is_string($sourceName)) {
            throw new InvalidArgumentException(sprintf(
                'Argument $sourceName must be a string, %s given',
                is_object($sourceName) ? get_class($sourceName) : gettype($sourceName)
            ));
        }

        if (!is_string($sourceId)) {
            throw new InvalidArgumentException(sprintf(
                'Argument $sourceId must be a string, %s given',
                is_object($sourceId) ? get_class($sourceId) : gettype($sourceId)
            ));
        }

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
