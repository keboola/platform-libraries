<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Staging;

use Keboola\InputMapping\Exception\StagingException;

abstract class AbstractDefinition
{
    public const STAGING_FILE = 'file';
    public const STAGING_TABLE = 'table';

    /**
     * @param class-string $fileStagingClass
     * @param class-string $tableStagingClass
     */
    public function __construct(
        protected readonly string $name,
        protected string $fileStagingClass,
        protected string $tableStagingClass,
        protected ?ProviderInterface $fileDataProvider = null,
        protected ?ProviderInterface $fileMetadataProvider = null,
        protected ?ProviderInterface $tableDataProvider = null,
        protected ?ProviderInterface $tableMetadataProvider = null
    ) {
    }

    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return class-string
     */
    abstract public function getFileStagingClass(): string;

    /**
     * @return class-string
     */
    abstract public function getTableStagingClass(): string;

    public function getFileDataProvider(): ?ProviderInterface
    {
        return $this->fileDataProvider;
    }

    public function setFileDataProvider(?ProviderInterface $fileDataProvider): void
    {
        $this->fileDataProvider = $fileDataProvider;
    }

    public function getFileMetadataProvider(): ?ProviderInterface
    {
        return $this->fileMetadataProvider;
    }

    public function setFileMetadataProvider(?ProviderInterface $fileMetadataProvider): AbstractDefinition
    {
        $this->fileMetadataProvider = $fileMetadataProvider;
        return $this;
    }

    public function getTableDataProvider(): ?ProviderInterface
    {
        return $this->tableDataProvider;
    }

    public function setTableDataProvider(?ProviderInterface $tableDataProvider): void
    {
        $this->tableDataProvider = $tableDataProvider;
    }

    public function getTableMetadataProvider(): ?ProviderInterface
    {
        return $this->tableMetadataProvider;
    }

    public function setTableMetadataProvider(ProviderInterface $tableMetadataProvider): void
    {
        $this->tableMetadataProvider = $tableMetadataProvider;
    }

    public function validateFor(string $stagingType): void
    {
        switch ($stagingType) {
            case self::STAGING_FILE:
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
