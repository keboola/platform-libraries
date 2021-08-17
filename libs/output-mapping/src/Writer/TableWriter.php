<?php

namespace Keboola\OutputMapping\Writer;

use Exception;
use Keboola\OutputMapping\DeferredTasks\LoadTableQueue;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\Table\TableWriterFactory;

/**
 * This is not actual table writer but rather a factory proxy that transparently
 * decides which real table writer to use based on conditions implemented in  TableWriterFactory.
 *
 * The name & the interface of the class is intentionally kept the same as original, to not break the compatibility
 * on implementer side.
 */
class TableWriter extends AbstractWriter
{
    const SYSTEM_METADATA_PROVIDER = 'system';
    const KBC_LAST_UPDATED_BY_BRANCH_ID = 'KBC.lastUpdatedBy.branch.id';
    const KBC_LAST_UPDATED_BY_CONFIGURATION_ROW_ID = 'KBC.lastUpdatedBy.configurationRow.id';
    const KBC_LAST_UPDATED_BY_CONFIGURATION_ID = 'KBC.lastUpdatedBy.configuration.id';
    const KBC_LAST_UPDATED_BY_COMPONENT_ID = 'KBC.lastUpdatedBy.component.id';
    const KBC_CREATED_BY_BRANCH_ID = 'KBC.createdBy.branch.id';
    const KBC_CREATED_BY_CONFIGURATION_ROW_ID = 'KBC.createdBy.configurationRow.id';
    const KBC_CREATED_BY_CONFIGURATION_ID = 'KBC.createdBy.configuration.id';
    const KBC_CREATED_BY_COMPONENT_ID = 'KBC.createdBy.component.id';
    const TAG_STAGING_FILES_FEATURE = 'tag-staging-files';

    /** @var TableWriterFactory */
    private $writerFactory;

    public function __construct(StrategyFactory $strategyFactory)
    {
        parent::__construct($strategyFactory);

        $this->writerFactory = new TableWriterFactory($strategyFactory);
    }

    /**
     * @param string $source
     * @param array $configuration
     * @param array $systemMetadata
     * @param string $stagingStorageOutput
     * @return LoadTableQueue
     * @throws Exception
     */
    public function uploadTables($source, array $configuration, array $systemMetadata, $stagingStorageOutput)
    {
        $realWriter = $this->writerFactory->createTableWriter();
        $realWriter->setFormat($this->format);

        $this->logger->info(sprintf('Using %s to upload tables', $this->resolveRealWriterName($realWriter)));

        return $realWriter->uploadTables($source, $configuration, $systemMetadata, $stagingStorageOutput);
    }

    private function resolveRealWriterName($realWriter)
    {
        $writerVariantName = get_class($realWriter);
        $writerVariantName = substr($writerVariantName, strrpos($writerVariantName, '\\') + 1); // remove namespace
        $writerVariantName = substr($writerVariantName, strlen('TableWriter')); // remove TableWriter prefix from class name

        return 'TableWriter '.$writerVariantName;
    }
}
