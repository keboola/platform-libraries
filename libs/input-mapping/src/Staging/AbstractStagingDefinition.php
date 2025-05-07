<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\Exception\StagingException;

/**
 * @template T_TABLE_STAGING of object
 * @template T_FILE_STAGING of object
 */
abstract class AbstractStagingDefinition
{
    public const STAGING_FILE = 'file';
    public const STAGING_TABLE = 'table';

    /**
     * @param class-string<T_FILE_STAGING> $fileStagingClass
     * @param class-string<T_TABLE_STAGING> $tableStagingClass
     */
    public function __construct(
        protected readonly string $name,
        protected readonly string $fileStagingClass,
        protected readonly string $tableStagingClass,
        protected ?StagingInterface $fileDataStaging = null,
        protected ?StagingInterface $fileMetadataStaging = null,
        protected ?StagingInterface $tableDataStaging = null,
        protected ?StagingInterface $tableMetadataStaging = null,
    ) {
    }

    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return class-string<T_FILE_STAGING>
     */
    abstract public function getFileStagingClass(): string;

    /**
     * @return class-string<T_TABLE_STAGING>
     */
    abstract public function getTableStagingClass(): string;

    public function getFileDataStaging(): ?StagingInterface
    {
        return $this->fileDataStaging;
    }

    public function setFileDataStaging(?StagingInterface $fileDataProvider): void
    {
        $this->fileDataStaging = $fileDataProvider;
    }

    public function getFileMetadataStaging(): ?StagingInterface
    {
        return $this->fileMetadataStaging;
    }

    public function setFileMetadataStaging(?StagingInterface $fileMetadataProvider): void
    {
        $this->fileMetadataStaging = $fileMetadataProvider;
    }

    public function getTableDataStaging(): ?StagingInterface
    {
        return $this->tableDataStaging;
    }

    public function setTableDataStaging(?StagingInterface $tableDataProvider): void
    {
        $this->tableDataStaging = $tableDataProvider;
    }

    public function getTableMetadataStaging(): ?StagingInterface
    {
        return $this->tableMetadataStaging;
    }

    public function setTableMetadataStaging(StagingInterface $tableMetadataProvider): void
    {
        $this->tableMetadataStaging = $tableMetadataProvider;
    }

    public function validateFor(string $stagingType): void
    {
        switch ($stagingType) {
            case self::STAGING_FILE:
                if (empty($this->fileDataStaging)) {
                    throw new StagingException(
                        sprintf('Undefined file data provider in "%s" staging.', $this->name),
                    );
                }
                if (empty($this->fileMetadataStaging)) {
                    throw new StagingException(
                        sprintf('Undefined file metadata provider in "%s" staging.', $this->name),
                    );
                }
                break;
            case self::STAGING_TABLE:
                if (empty($this->tableDataStaging)) {
                    throw new StagingException(
                        sprintf('Undefined table data provider in "%s" staging.', $this->name),
                    );
                }
                if (empty($this->tableMetadataStaging)) {
                    throw new StagingException(
                        sprintf('Undefined table metadata provider in "%s" staging.', $this->name),
                    );
                }
                break;
            default:
                throw new StagingException(sprintf('Unknown staging type: "%s".', $stagingType));
        }
    }
}
