<?php

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\Exception\StagingException;

class Definition
{
    const STAGING_FILE = 'file';
    const STAGING_TABLE = 'table';

    /** @var string */
    private $name;

    /** @var string */
    private $tableStagingClass;

    /** @var string */
    private $fileStagingClass;

    /** @var CapabilityInterface */
    private $tableDataCapability;

    /** @var CapabilityInterface */
    private $tableMetadataCapability;

    /** @var CapabilityInterface */
    private $fileDataCapability;

    /** @var CapabilityInterface */
    private $fileMetadataCapability;

    public function __construct(
        $name,
        $fileStagingClass,
        $tableStagingClass,
        $fileDataCapability = null,
        $fileMetadataCapability = null,
        $tableDataCapability = null,
        $tableMetadataCapability = null
    ) {
        $this->name = $name;
        $this->fileStagingClass = $fileStagingClass;
        $this->tableStagingClass = $tableStagingClass;
        $this->fileDataCapability = $fileDataCapability;
        $this->fileMetadataCapability = $fileMetadataCapability;
        $this->tableDataCapability = $tableDataCapability;
        $this->tableMetadataCapability = $tableMetadataCapability;
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
    public function getFileStagingClass()
    {
        return $this->fileStagingClass;
    }

    /**
     * @return string
     */
    public function getTableStagingClass()
    {
        return $this->tableStagingClass;
    }

    /**
     * @return CapabilityInterface
     */
    public function getFileDataCapability()
    {
        return $this->fileDataCapability;
    }

    /**
     * @param CapabilityInterface $fileDataCapability
     */
    public function setFileDataCapability($fileDataCapability)
    {
        $this->fileDataCapability = $fileDataCapability;
    }

    /**
     * @return CapabilityInterface
     */
    public function getFileMetadataCapability()
    {
        return $this->fileMetadataCapability;
    }

    /**
     * @param CapabilityInterface $fileMetadataCapability
     * @return Definition
     */
    public function setFileMetadataCapability($fileMetadataCapability)
    {
        $this->fileMetadataCapability = $fileMetadataCapability;
        return $this;
    }

    /**
     * @return CapabilityInterface
     */
    public function getTableDataCapability()
    {
        return $this->tableDataCapability;
    }

    /**
     * @param CapabilityInterface $tableDataCapability
     */
    public function setTableDataCapability($tableDataCapability)
    {
        $this->tableDataCapability = $tableDataCapability;
    }

    /**
     * @return CapabilityInterface
     */
    public function getTableMetadataCapability()
    {
        return $this->tableMetadataCapability;
    }

    /**
     * @param CapabilityInterface $tableMetadataCapability
     */
    public function setTableMetadataCapability($tableMetadataCapability)
    {
        $this->tableMetadataCapability = $tableMetadataCapability;
    }

    public function validateFor($stagingType)
    {
        switch ($stagingType) {
            case self::STAGING_FILE:
                if (empty($this->fileStagingClass)) {
                    throw new StagingException(sprintf('Undefined file class in "%s" staging.', $this->name));
                }
                if (empty($this->fileDataCapability)) {
                    throw new StagingException(
                        sprintf('Undefined file data capability in "%s" staging.', $this->name)
                    );
                }
                if (empty($this->fileMetadataCapability)) {
                    throw new StagingException(
                        sprintf('Undefined file metadata capability in "%s" staging.', $this->name)
                    );
                }
                break;
            case self::STAGING_TABLE:
                if (empty($this->tableStagingClass)) {
                    throw new StagingException(sprintf('Undefined table class in "%s" staging.', $this->name));
                }
                if (empty($this->tableDataCapability)) {
                    throw new StagingException(
                        sprintf('Undefined table data capability in "%s" staging.', $this->name)
                    );
                }
                if (empty($this->tableMetadataCapability)) {
                    throw new StagingException(
                        sprintf('Undefined table metadata capability in "%s" staging.', $this->name)
                    );
                }
                break;
            default:
                throw new StagingException(sprintf('Unknown staging type: "%s".', $stagingType));
        }
    }
}
