<?php

namespace Keboola\InputMapping;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Helper\BuildQueryFromConfigurationHelper;
use Keboola\InputMapping\Helper\InputBucketValidator;
use Keboola\InputMapping\Helper\SourceRewriteHelper;
use Keboola\InputMapping\Helper\TagsRewriteHelper;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputFileState;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\InputMapping\Table\TableDefinitionResolver;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\LoggerInterface;

class Reader
{
    /**
     * @var ClientWrapper
     */
    protected $clientWrapper;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var StrategyFactory
     */
    private $strategyFactory;

    /**
     * @param StrategyFactory $strategyFactory
     */
    public function __construct(
        StrategyFactory $strategyFactory
    ) {
        $this->logger = $strategyFactory->getLogger();
        $this->clientWrapper = $strategyFactory->getClientWrapper();
        $this->strategyFactory = $strategyFactory;
    }

    /**
     * @param $configuration array
     * @param $destination string Relative path to the destination directory
     * @param $stagingType string
     * @param InputFileStateList $filesState list of input mapping file states
     * @return InputTableStateList
     */
    public function downloadFiles($configuration, $destination, $stagingType, InputFileStateList $filesState)
    {
        $strategy = $this->strategyFactory->getFileInputStrategy($stagingType, $filesState);
        if (!$configuration) {
            return new InputFileStateList([]);
        } elseif (!is_array($configuration)) {
            throw new InvalidInputException("File download configuration is not an array.");
        }
        return $strategy->downloadFiles($configuration, $destination);
    }

    /**
     * @param InputTableOptionsList $tablesDefinition list of input mappings
     * @param InputTableStateList $tablesState list of input mapping table states
     * @param string $destination destination folder
     * @param string $stagingType
     * @param ReaderOptions $readerOptions
     * @return InputTableStateList
     */
    public function downloadTables(
        InputTableOptionsList $tablesDefinition,
        InputTableStateList $tablesState,
        $destination,
        $stagingType,
        ReaderOptions $readerOptions
    ) {
        $tableResolver = new TableDefinitionResolver($this->clientWrapper->getBasicClient(), $this->logger);
        $tablesState = SourceRewriteHelper::rewriteTableStatesDestinations(
            $tablesState,
            $this->clientWrapper,
            $this->logger
        );
        $tablesDefinition = $tableResolver->resolve($tablesDefinition);
        $strategy = $this->strategyFactory->getTableInputStrategy($stagingType, $destination, $tablesState);
        if ($readerOptions->devInputsDisabled()) {
            InputBucketValidator::checkDevBuckets(
                $tablesDefinition,
                $this->clientWrapper
            );
        }
        $tablesDefinition = SourceRewriteHelper::rewriteTableOptionsSources(
            $tablesDefinition,
            $this->clientWrapper,
            $this->logger
        );
        return $strategy->downloadTables($tablesDefinition->getTables());
    }

    /**
     * Get parent runId to the current runId (defined by SAPI client)
     * @param string $runId
     * @return string Parent part of hierarchical Id.
     */
    public static function getParentRunId($runId)
    {
        if (!empty($runId)) {
            if (($pos = strrpos($runId, '.')) === false) {
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

    /**
     * @return array
     */
    public static function getFiles(
        array $fileConfiguration,
        InputFileState $fileState,
        ClientWrapper $clientWrapper,
        LoggerInterface $logger
    ) {
        $fileConfiguration = TagsRewriteHelper::rewriteFileTags(
            $fileConfiguration,
            $clientWrapper,
            $logger
        );
        $storageClient = $clientWrapper->getBasicClient();

        if (isset($fileConfiguration["query"]) && $clientWrapper->hasBranch()) {
            throw new InvalidInputException(
                "Invalid file mapping, the 'query' attribute is unsupported in the dev/branch context."
            );
        }
        $options = new ListFilesOptions();
        if (empty($fileConfiguration['tags']) && empty($fileConfiguration['query'])
            && empty($fileConfiguration['source']['tags'])
        ) {
            throw new InvalidInputException("Invalid file mapping, 'tags', 'query' and 'source.tags' are empty.");
        }
        if (!empty($fileConfiguration['tags']) && !empty($fileConfiguration['source']['tags'])) {
            throw new InvalidInputException("Invalid file mapping, both 'tags' and 'source.tags' cannot be set.");
        }
        if (!empty($fileConfiguration['query']) && isset($fileConfiguration['changed_since'])) {
            throw new InvalidInputException('Invalid file mapping, "changed_since" is not supported for query mappings');
        }
        if (!empty($fileConfiguration['filter_by_run_id'])) {
            $options->setRunId(Reader::getParentRunId($storageClient->getRunId()));
        }
        if (isset($fileConfiguration["tags"]) && count($fileConfiguration["tags"])) {
            $options->setTags($fileConfiguration["tags"]);
        }
        if (isset($fileConfiguration["query"]) || isset($fileConfiguration['source']['tags'])) {
            $options->setQuery(
                BuildQueryFromConfigurationHelper::buildQuery($fileConfiguration)
            );
        }
        if (empty($fileConfiguration["limit"])) {
            $fileConfiguration["limit"] = 100;
        }
        $options->setLimit($fileConfiguration["limit"]);

        if (isset($fileConfiguration['changed_since']) && $fileConfiguration['changed_since'] === 'adaptive') {
            $options->setSinceId($fileState->getLastImportId());
        }
        $files = $storageClient->listFiles($options);
        return $files;
    }
}
