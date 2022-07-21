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

    /** @var ProviderInterface */
    private $tableDataProvider;

    /** @var ProviderInterface */
    private $tableMetadataProvider;

    /** @var ProviderInterface */
    private $fileDataProvider;

    /** @var ProviderInterface */
    private $fileMetadataProvider;

    public function __construct(
        $name,
        $fileStagingClass,
        $tableStagingClass,
        $fileDataProvider = null,
        $fileMetadataProvider = null,
        $tableDataProvider = null,
        $tableMetadataProvider = null
    ) {
        $this->name = $name;
        $this->fileStagingClass = $fileStagingClass;
        $this->tableStagingClass = $tableStagingClass;
        $this->fileDataProvider = $fileDataProvider;
        $this->fileMetadataProvider = $fileMetadataProvider;
        $this->tableDataProvider = $tableDataProvider;
        $this->tableMetadataProvider = $tableMetadataProvider;
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
     * @return ProviderInterface
     */
    public function getFileDataProvider()
    {
        return $this->fileDataProvider;
    }

    /**
     * @param ProviderInterface $fileDataProvider
     */
    public function setFileDataProvider($fileDataProvider)
    {
        $this->fileDataProvider = $fileDataProvider;
    }

    /**
     * @return ProviderInterface
     */
    public function getFileMetadataProvider()
    {
        return $this->fileMetadataProvider;
    }

    /**
     * @param ProviderInterface $fileMetadataProvider
     * @return Definition
     */
    public function setFileMetadataProvider($fileMetadataProvider)
    {
        $this->fileMetadataProvider = $fileMetadataProvider;
        return $this;
    }

    /**
     * @return ProviderInterface
     */
    public function getTableDataProvider()
    {
        return $this->tableDataProvider;
    }

    /**
     * @param ProviderInterface $tableDataProvider
     */
    public function setTableDataProvider($tableDataProvider)
    {
        $this->tableDataProvider = $tableDataProvider;
    }

    /**
     * @return ProviderInterface
     */
    public function getTableMetadataProvider()
    {
        return $this->tableMetadataProvider;
    }

    /**
     * @param ProviderInterface $tableMetadataProvider
     */
    public function setTableMetadataProvider($tableMetadataProvider)
    {
        $this->tableMetadataProvider = $tableMetadataProvider;
    }

    public function validateFor($stagingType)
    {
        switch ($stagingType) {
            case self::STAGING_FILE:
                if (empty($this->fileStagingClass)) {
                    throw new StagingException(sprintf('Undefined file class in "%s" staging.', $this->name));
                }
                if (empty($this->fileDataProvider)) {
                    throw new StagingException(
                        sprintf('Undefined file data provider in "%s" staging.', $this->name)
                    );
                }
                if (empty($this->fileMetadataProvider)) {
                    throw new StagingException(
                        sprintf('Undefined file metadata provider in "%s" staging.', $this->name)
                    );
                }
                break;
            case self::STAGING_TABLE:
                if (empty($this->tableStagingClass)) {
                    throw new StagingException(sprintf('Undefined table class in "%s" staging.', $this->name));
                }
                if (empty($this->tableDataProvider)) {
                    throw new StagingException(
                        sprintf('Undefined table data provider in "%s" staging.', $this->name)
                    );
                }
                if (empty($this->tableMetadataProvider)) {
                    throw new StagingException(
                        sprintf('Undefined table metadata provider in "%s" staging.', $this->name)
                    );
                }
                break;
            default:
                throw new StagingException(sprintf('Unknown staging type: "%s".', $stagingType));
        }
    }
}
