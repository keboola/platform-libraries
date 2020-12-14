<?php

namespace Keboola\InputMapping;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Helper\BuildQueryFromConfigurationHelper;
use Keboola\InputMapping\Helper\ManifestWriter;
use Keboola\InputMapping\Helper\SourceRewriteHelper;
use Keboola\InputMapping\Helper\TagsRewriteHelper;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\TableDefinitionResolver;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
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
     * @var
     */
    protected $format = 'json';

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var StrategyFactory
     */
    private $strategyFactory;

    /**
     * @param ClientWrapper $clientWrapper
     * @param StrategyFactory $strategyFactory
     */
    public function __construct(
        ClientWrapper $clientWrapper,
        StrategyFactory $strategyFactory
    ) {
        $this->logger = $strategyFactory->getLogger();
        $this->clientWrapper = $clientWrapper;
        $this->strategyFactory = $strategyFactory;
    }

    /**
     * @return ManifestWriter
     */
    protected function getManifestWriter()
    {
        return new ManifestWriter($this->clientWrapper->getBasicClient(), $this->getFormat());
    }

    /**
     * @return string
     */
    public function getFormat()
    {
        return $this->format;
    }

    /**
     * @param string $format
     * @return $this
     */
    public function setFormat($format)
    {
        $this->format = $format;

        return $this;
    }

    /**
     * @param $configuration array
     * @param $destination string Destination directory
     * @param $stagingType string
     */
    public function downloadFiles($configuration, $destination, $stagingType)
    {
        $strategy = $this->strategyFactory->getFileStrategy($stagingType);
        if (!$configuration) {
            return;
        } elseif (!is_array($configuration)) {
            throw new InvalidInputException("File download configuration is not an array.");
        }
        $strategy->downloadFiles($configuration, $destination);
    }

    /**
     * @param InputTableOptionsList $tablesDefinition list of input mappings
     * @param InputTableStateList $tablesState list of input mapping states
     * @param string $destination destination folder
     * @param string $stagingType
     * @return InputTableStateList
     * @throws ClientException
     * @throws Exception
     */
    public function downloadTables(
        InputTableOptionsList $tablesDefinition,
        InputTableStateList $tablesState,
        $destination,
        $stagingType
    ) {
        $tableResolver = new TableDefinitionResolver($this->clientWrapper->getBasicClient(), $this->logger);
        $tablesState = SourceRewriteHelper::rewriteTableStatesDestinations(
            $tablesState,
            $this->clientWrapper,
            $this->logger
        );
        $tablesDefinition = $tableResolver->resolve($tablesDefinition);
        $strategy = $this->strategyFactory->getTableStrategy($stagingType, $destination, $tablesState);
        $tablesDefinition = SourceRewriteHelper::rewriteTableOptionsDestinations(
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
                "Invalid file mapping, 'query' attribute is restricted for dev/branch context."
            );
        }

        if (isset($fileConfiguration["processed_tags"]) && $clientWrapper->hasBranch()) {
            throw new InvalidInputException("Invalid file mapping, 'processed_tags' attribute is restricted for dev/branch context.");
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
        $files = $storageClient->listFiles($options);

        return $files;
    }
}
