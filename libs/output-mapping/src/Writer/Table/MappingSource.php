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

    /** @var bool */
    private $isSliced;

    /** @var null|SplFileInfo */
    private $manifestFile;

    /** @var null|array */
    private $mapping;

    /**
     * @param string $sourceName
     * @param string $sourceId
     * @param bool $isSliced
     */
    public function __construct(
        $sourceName,
        $sourceId,
        $isSliced,
        SplFileInfo $manifestFile = null,
        array $mapping = null
    ) {
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

        if (!is_bool($isSliced)) {
            throw new InvalidArgumentException(sprintf(
                'Argument $isSliced must be a boolean, %s given',
                is_object($isSliced) ? get_class($isSliced) : gettype($isSliced)
            ));
        }

        $this->name = $sourceName;
        $this->id = $sourceId;
        $this->isSliced = $isSliced;
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
     * @return bool
     */
    public function isSliced()
    {
        return $this->isSliced;
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
