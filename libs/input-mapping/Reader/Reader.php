<?php

namespace Keboola\InputMapping\Reader;

use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\InputMapping\Reader\Strategy\StrategyFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Psr\Log\LoggerInterface;
use Symfony\Component\Filesystem\Filesystem;

class Reader
{
    /**
     * @var Client
     */
    protected $client;
    /**
     * @var
     */
    protected $format = 'json';
    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var WorkspaceProviderInterface
     */
    private $workspaceProvider;

    /** @var ManifestWriter */
    protected $manifestWriter;

    /**
     * @return Client
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @param Client $client
     * @return $this
     */
    public function setClient(Client $client)
    {
        $this->client = $client;

        return $this;
    }

    /**
     * @param Client $client
     * @param LoggerInterface $logger
     */
    public function __construct(Client $client, LoggerInterface $logger, WorkspaceProviderInterface $workspaceProvider)
    {
        $this->logger = $logger;
        $this->setClient($client);
        $this->workspaceProvider = $workspaceProvider;
        $this->manifestWriter = new ManifestWriter($client, $this->format);
    }

    /**
     * @param $configuration array
     * @param $destination string Destination directory
     */
    public function downloadFiles($configuration, $destination)
    {
        $fs = new Filesystem();
        $fs->mkdir($destination);
        if (!$configuration) {
            return;
        } elseif (!is_array($configuration)) {
            throw new InvalidInputException("File download configuration is not an array.");
        }
        $storageClient = $this->getClient();
        $fileOptions = new GetFileOptions();
        $fileOptions->setFederationToken(true);

        foreach ($configuration as $fileConfiguration) {
            $files = $this->getFiles($fileConfiguration);
            foreach ($files as $file) {
                $fileInfo = $storageClient->getFile($file['id'], $fileOptions);
                $fileDestinationPath = sprintf('%s/%s_%s', $destination, $fileInfo['id'], $fileInfo["name"]);
                $this->logger->info(sprintf('Fetching file %s (%s).', $fileInfo['name'], $file['id']));
                try {
                    $this->downloadFile($fileInfo, $fileDestinationPath);
                } catch (\Exception $e) {
                    throw new InputOperationException(
                        sprintf('Failed to download file %s (%s).', $fileInfo['name'], $file['id']),
                        0,
                        $e
                    );
                }
                $this->logger->info(sprintf('Fetched file %s (%s).', $fileInfo['name'], $file['id']));
            }
        }
        $this->logger->info('All files were fetched.');
    }

    /**
     * @param $fileConfiguration
     * @return array
     */
    public function getFiles($fileConfiguration)
    {
        $options = new ListFilesOptions();
        if (empty($fileConfiguration['tags']) && empty($fileConfiguration['query'])) {
            throw new InvalidInputException("Invalid file mapping, both 'tags' and 'query' are empty.");
        }
        if (!empty($fileConfiguration['filter_by_run_id'])) {
            $options->setRunId($this->getParentRunId());
        }
        if (isset($fileConfiguration["tags"]) && count($fileConfiguration["tags"])) {
            $options->setTags($fileConfiguration["tags"]);
        }
        if (isset($fileConfiguration["query"])) {
            $options->setQuery($fileConfiguration["query"]);
        }
        if (empty($fileConfiguration["limit"])) {
            $fileConfiguration["limit"] = 100;
        }
        $options->setLimit($fileConfiguration["limit"]);
        $files = $this->getClient()->listFiles($options);

        return $files;
    }

    /**
     * @param array $fileInfo array file info from Storage API
     * @param string $fileDestinationPath string Destination file path
     * @throws \Exception
     */
    protected function downloadFile($fileInfo, $fileDestinationPath)
    {
        if ($fileInfo['isSliced']) {
            $this->getClient()->downloadSlicedFile($fileInfo['id'], $fileDestinationPath);
        } else {
            $this->getClient()->downloadFile($fileInfo['id'], $fileDestinationPath);
        }
        $this->manifestWriter->writeFileManifest($fileInfo, $fileDestinationPath . ".manifest");
    }

    /**
     * @param InputTableOptionsList $tablesDefinition list of input mappings
     * @param InputTableStateList $tablesState list of input mapping states
     * @param string $destination destination folder
     * @param string $storage
     * @return InputTableStateList
     * @throws \Keboola\StorageApi\ClientException
     * @throws \Keboola\StorageApi\Exception
     */
    public function downloadTables(InputTableOptionsList $tablesDefinition, InputTableStateList $tablesState, $destination, $storage = 'local')
    {
        $tableResolver = new TableDefinitionResolver($this->client, $this->logger);
        $strategyFactory = new StrategyFactory($this->client, $this->logger, $this->workspaceProvider, $tablesState, $destination);

        $tablesDefinition = $tableResolver->resolve($tablesDefinition);
        $strategy = $strategyFactory->getStrategy($storage);

        return $strategy->downloadTables($tablesDefinition->getTables());
    }

    /**
     * Get parent runId to the current runId (defined by SAPI client)
     * @return string Parent part of hierarchical Id.
     */
    public function getParentRunId()
    {
        $runId = $this->client->getRunId();
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
}
