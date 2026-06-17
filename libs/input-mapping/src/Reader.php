<?php

declare(strict_types=1);

namespace Keboola\InputMapping;

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
use Keboola\InputMapping\Table\Strategy\TableLoadQueueInterface;
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
        // Behave exactly like an outside caller would: start the loads, then complete them
        // through the queue's stamped strategy identity.
        $queue = $this->prepareAndExecuteTableLoads(
            $tablesDefinition,
            $tablesState,
            $destination,
            $readerOptions,
        );
        return $this->waitForTableLoadCompletion($queue);
    }

    /**
     * Execute only the start phase (plan + submit) of table loading.
     * Returns a queue for later completion with the strategy's waitForTableLoadCompletion().
     *
     * @param InputTableOptionsList $tablesDefinition list of input mappings
     * @param InputTableStateList $tablesState list of input mapping table states
     * @param string $destination destination folder
     * @param ReaderOptions $readerOptions
     */
    public function prepareAndExecuteTableLoads(
        InputTableOptionsList $tablesDefinition,
        InputTableStateList $tablesState,
        string $destination,
        ReaderOptions $readerOptions,
    ): TableLoadQueueInterface {
        $tablesState = $this->rewriteTableStatesDestinations($tablesState);

        $tablesDefinition = $this->createTableResolver()->resolve($tablesDefinition);
        $strategy = $this->strategyFactory->getTableInputStrategy($destination, $tablesState);
        $tablesDefinition = $this->validateAndRewriteDevBuckets($tablesDefinition, $readerOptions);

        return $strategy->prepareAndExecuteTableLoads(
            $tablesDefinition->getTables(),
            $readerOptions->preserveWorkspace(),
        );
    }

    /**
     * Finish phase for a queue returned by prepareAndExecuteTableLoads():
     * awaits the jobs, materializes data/manifests and builds the Result.
     */
    public function waitForTableLoadCompletion(TableLoadQueueInterface $queue): Result
    {
        $strategy = $this->strategyFactory->getTableInputStrategy(
            $queue->getDestination(),
            new InputTableStateList([]),
        );
        if (!is_a($strategy, $queue->getStrategyClass())) {
            throw new InputOperationException(sprintf(
                'Cannot complete table loads: the queue was created by "%s" but the current staging provides "%s".',
                $queue->getStrategyClass(),
                get_class($strategy),
            ));
        }
        return $strategy->waitForTableLoadCompletion($queue);
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
