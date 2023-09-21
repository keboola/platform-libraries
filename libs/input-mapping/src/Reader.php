<?php

declare(strict_types=1);

namespace Keboola\InputMapping;

use Keboola\InputMapping\File\Options\InputFileOptions;
use Keboola\InputMapping\File\Options\RewrittenInputFileOptions;
use Keboola\InputMapping\Helper\InputBucketValidator;
use Keboola\InputMapping\Helper\TableRewriteHelperFactory;
use Keboola\InputMapping\Helper\TagsRewriteHelperFactory;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Table\Result;
use Keboola\InputMapping\Table\Strategy\AbstractStrategy as TableAbstractStrategy;
use Keboola\InputMapping\Table\TableDefinitionResolver;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class Reader
{
    protected ClientWrapper $clientWrapper;
    private LoggerInterface $logger;
    private StrategyFactory $strategyFactory;

    public function __construct(StrategyFactory $strategyFactory)
    {
        $this->logger = $strategyFactory->getLogger();
        $this->clientWrapper = $strategyFactory->getClientWrapper();
        $this->strategyFactory = $strategyFactory;
    }

    /**
     * @param $configuration array
     * @param $destination string Relative path to the destination directory
     * @param $stagingType string
     * @param InputFileStateList $filesState list of input mapping file states
     * @return InputFileStateList
     */
    public function downloadFiles(
        array $configuration,
        string $destination,
        string $stagingType,
        InputFileStateList $filesState,
    ): InputFileStateList {
        $strategy = $this->strategyFactory->getFileInputStrategy($stagingType, $filesState);
        if (!$configuration) {
            return new InputFileStateList([]);
        }
        return $strategy->downloadFiles($configuration, $destination);
    }

    /**
     * @param InputTableOptionsList $tablesDefinition list of input mappings
     * @param InputTableStateList $tablesState list of input mapping table states
     * @param string $destination destination folder
     * @param string $stagingType
     * @param ReaderOptions $readerOptions
     * @return Result
     */
    public function downloadTables(
        InputTableOptionsList $tablesDefinition,
        InputTableStateList $tablesState,
        string $destination,
        string $stagingType,
        ReaderOptions $readerOptions,
    ): Result {
        $tableResolver = new TableDefinitionResolver(
            $this->clientWrapper->getTableAndFileStorageClient(),
            $this->logger,
        );
        $tablesState = TableRewriteHelperFactory::getTableRewriteHelper(
            $this->clientWrapper->getClientOptionsReadOnly(),
        )->rewriteTableStatesDestinations(
            $tablesState,
            $this->clientWrapper,
            $this->logger,
        );
        $tablesDefinition = $tableResolver->resolve($tablesDefinition);
        $strategy = $this->strategyFactory->getTableInputStrategy($stagingType, $destination, $tablesState);
        if ($readerOptions->devInputsDisabled()
            && !$this->clientWrapper->getClientOptionsReadOnly()->useBranchStorage()
        ) {
            /* this is irrelevant for protected branch projects, because dev & prod buckets have same name, thus there
            is no difference which one is stored in the configuration */
            InputBucketValidator::checkDevBuckets(
                $tablesDefinition,
                $this->clientWrapper,
            );
        }
        $tablesDefinition = TableRewriteHelperFactory::getTableRewriteHelper(
            $this->clientWrapper->getClientOptionsReadOnly(),
        )->rewriteTableOptionsSources(
            $tablesDefinition,
            $this->clientWrapper,
            $this->logger,
        );
        /** @var TableAbstractStrategy $strategy */
        return $strategy->downloadTables($tablesDefinition->getTables(), $readerOptions->preserveWorkspace());
    }

    /**
     * Get parent runId to the current runId (defined by SAPI client)
     * @param string $runId
     * @return string Parent part of hierarchical Id.
     */
    public static function getParentRunId(string $runId): string
    {
        if (!empty($runId)) {
            $pos = strrpos($runId, '.');
            if ($pos === false) {
                // there is no parent
                $parentRunId = $runId;
            } else {
                $parentRunId = substr($runId, 0, $pos);
            }
        } else {
            // there is no runId
            $parentRunId = '';
        }
        return $parentRunId;
    }

    public static function getFiles(
        array $fileConfiguration,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
    ): RewrittenInputFileOptions {
        $fileOptions = new InputFileOptions(
            $fileConfiguration,
            $clientWrapper->isDevelopmentBranch(),
            (string) $clientWrapper->getTableAndFileStorageClient()->getRunId(),
        );
        $fileOptionsRewritten = TagsRewriteHelperFactory::getTagsRewriteHelper(
            $clientWrapper->getClientOptionsReadOnly(),
        )->rewriteFileTags(
            $fileOptions,
            $clientWrapper,
            $logger,
        );
        return $fileOptionsRewritten;
    }
}
