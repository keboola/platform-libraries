<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Writer;

use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\OutputMappingSettings;
use Keboola\OutputMapping\SystemMetadata;
use Keboola\OutputMapping\TableLoader;

class TableWriter extends AbstractWriter
{
    public const SYSTEM_METADATA_PROVIDER = 'system';
    public const KBC_LAST_UPDATED_BY_BRANCH_ID = 'KBC.lastUpdatedBy.branch.id';
    public const KBC_LAST_UPDATED_BY_CONFIGURATION_ROW_ID = 'KBC.lastUpdatedBy.configurationRow.id';
    public const KBC_LAST_UPDATED_BY_CONFIGURATION_ID = 'KBC.lastUpdatedBy.configuration.id';
    public const KBC_LAST_UPDATED_BY_COMPONENT_ID = 'KBC.lastUpdatedBy.component.id';
    public const KBC_CREATED_BY_BRANCH_ID = 'KBC.createdBy.branch.id';
    public const KBC_CREATED_BY_CONFIGURATION_ROW_ID = 'KBC.createdBy.configurationRow.id';
    public const KBC_CREATED_BY_CONFIGURATION_ID = 'KBC.createdBy.configuration.id';
    public const KBC_CREATED_BY_COMPONENT_ID = 'KBC.createdBy.component.id';
    public const TAG_STAGING_FILES_FEATURE = 'tag-staging-files';
    public const OUTPUT_MAPPING_SLICE_FEATURE = 'output-mapping-slice';

    public function uploadTables(
        string $sourcePathPrefix,
        array $configuration,
        array $systemMetadata,
        string $stagingStorageOutput,
        bool $isFailedJob,
        string $dataTypeSupport,
    ): LoadTableQueue {
        // TODO: this will be moved to caller
        $systemMetadata = new SystemMetadata($systemMetadata);
        $configuration = new OutputMappingSettings(
            $configuration,
            $sourcePathPrefix,
            $this->clientWrapper->getToken(),
            $isFailedJob,
            $dataTypeSupport,
        );

        $tableLoader = new TableLoader($this->logger, $this->clientWrapper, $this->strategyFactory);
        return $tableLoader->uploadTables(
            $stagingStorageOutput,
            $configuration,
            $systemMetadata,
        );
    }
}
