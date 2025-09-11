<?php

declare(strict_types=1);

namespace Keboola\InputMapping;

use InvalidArgumentException;
use Keboola\InputMapping\Exception\InputOperationException;
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
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptionsList;
use Keboola\InputMapping\Table\Result;
use Keboola\InputMapping\Table\Strategy\AbstractStrategy as TableAbstractStrategy;
use Keboola\InputMapping\Table\Strategy\AbstractWorkspaceStrategy;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadQueue;
use Keboola\InputMapping\Table\TableDefinitionResolver;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class Reader
{
    public function __construct(
        private readonly ClientWrapper $clientWrapper,
        private readonly LoggerInterface $logger,
        private readonly StrategyFactory $strategyFactory,
    ) {
    }

    /**
     * @param $configuration array
     * @param $destination string Relative path to the destination directory
     * @param InputFileStateList $filesState list of input mapping file states
     * @return InputFileStateList
     */
    public function downloadFiles(
        array $configuration,
        string $destination,
        InputFileStateList $filesState,
    ): InputFileStateList {
        $strategy = $this->strategyFactory->getFileInputStrategy($filesState);
        if (!$configuration) {
            return new InputFileStateList([]);
        }
        return $strategy->downloadFiles($configuration, $destination);
    }

    private function createTableResolver(): TableDefinitionResolver
    {
        return new TableDefinitionResolver(
            $this->clientWrapper->getTableAndFileStorageClient(),
            $this->logger,
        );
    }

    private function rewriteTableStatesDestinations(InputTableStateList $tablesState): InputTableStateList
    {
        return TableRewriteHelperFactory::getTableRewriteHelper(
            $this->clientWrapper->getClientOptionsReadOnly(),
        )->rewriteTableStatesDestinations(
            $tablesState,
            $this->clientWrapper,
            $this->logger,
        );
    }

    private function validateAndRewriteDevBuckets(
        InputTableOptionsList $tablesDefinition,
        ReaderOptions $readerOptions,
    ): RewrittenInputTableOptionsList {
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

        return TableRewriteHelperFactory::getTableRewriteHelper(
            $this->clientWrapper->getClientOptionsReadOnly(),
        )->rewriteTableOptionsSources(
            $tablesDefinition,
            $this->clientWrapper,
            $this->logger,
        );
    }


    /**
     * @param InputTableOptionsList $tablesDefinition list of input mappings
     * @param InputTableStateList $tablesState list of input mapping table states
     * @param string $destination destination folder
     * @param ReaderOptions $readerOptions
     * @return Result
     */
    public function downloadTables(
        InputTableOptionsList $tablesDefinition,
        InputTableStateList $tablesState,
        string $destination,
        ReaderOptions $readerOptions,
    ): Result {
        $tablesState = $this->rewriteTableStatesDestinations($tablesState);

        $tablesDefinition =  $this->createTableResolver()->resolve($tablesDefinition);
        $strategy = $this->strategyFactory->getTableInputStrategy($destination, $tablesState);
        $tablesDefinition = $this->validateAndRewriteDevBuckets($tablesDefinition, $readerOptions);

        /** @var TableAbstractStrategy $strategy */
        return $strategy->downloadTables($tablesDefinition->getTables(), $readerOptions->preserveWorkspace());
    }

    /**
     * Execute only prepare and execute phases for workspace table loading
     * Returns WorkspaceLoadQueue for later completion with waitForTableLoadCompletion()
     *
     * @param InputTableOptionsList $tablesDefinition list of input mappings
     * @param InputTableStateList $tablesState list of input mapping table states
     * @param string $destination destination folder
     * @param ReaderOptions $readerOptions
     * @return WorkspaceLoadQueue
     * @throws InvalidArgumentException if strategy is not workspace-based
     */
    public function prepareAndExecuteTableLoads(
        InputTableOptionsList $tablesDefinition,
        InputTableStateList $tablesState,
        string $destination,
        ReaderOptions $readerOptions,
    ): WorkspaceLoadQueue {
        $tablesState = $this->rewriteTableStatesDestinations($tablesState);

        $tablesDefinition = $this->createTableResolver()->resolve($tablesDefinition);
        $strategy = $this->strategyFactory->getTableInputStrategy($destination, $tablesState);

        // Ensure we have a workspace strategy. For file this method is not yet implemented.
        if (!$strategy instanceof AbstractWorkspaceStrategy) {
            throw new InputOperationException(
                'prepareAndExecuteTableLoads() can only be used with workspace strategies',
            );
        }

        $tablesDefinition = $this->validateAndRewriteDevBuckets($tablesDefinition, $readerOptions);

        // Execute only Phase 1 & 2: Prepare and Execute
        // Let the strategy handle the planning internally
        return $strategy->prepareAndExecuteTableLoads(
            $tablesDefinition->getTables(),
            $readerOptions->preserveWorkspace(),
        );
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
