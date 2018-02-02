<?php

namespace Keboola\InputMapping\Reader;

use Aws\S3\S3Client;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\TableExporter;
use Keboola\StorageApi\HandlerStack;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
use Symfony\Component\Filesystem\Filesystem;
use GuzzleHttp\Client as HttpClient;

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
     * @return mixed
     */
    public function getFormat()
    {
        return $this->format;
    }

    /**
     * @param mixed $format
     * @return $this
     */
    public function setFormat($format)
    {
        $this->format = $format;
        return $this;
    }

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
    public function __construct(Client $client, LoggerInterface $logger)
    {
        $this->logger = $logger;
        $this->setClient($client);
    }

    /**
     * @param $configuration array
     * @param $destination string Destination directory
     */
    public function downloadFiles($configuration, $destination)
    {
        if (!$configuration) {
            return;
        } elseif (!is_array($configuration)) {
            throw new InvalidInputException("File download configuration is not an array.");
        }
        foreach ($configuration as $fileConfiguration) {
            $files = $this->getFiles($fileConfiguration);
            foreach ($files as $file) {
                $fileInfo = $this->getClient()->getFile($file["id"], (new GetFileOptions())->setFederationToken(true));
                $this->logger->info("Fetching file " . $fileInfo['name'] . " (" . $file["id"] . ").");
                try {
                    if ($fileInfo['isSliced']) {
                        $this->downloadSlicedFile($fileInfo, $destination);
                        $this->writeFileManifest(
                            $fileInfo,
                            $destination . "/" . $fileInfo["id"] . '_' . $fileInfo["name"] . ".manifest"
                        );
                    } else {
                        $this->downloadFile($fileInfo, $destination . "/" . $fileInfo["id"] . '_' . $fileInfo["name"]);
                        $this->writeFileManifest(
                            $fileInfo,
                            $destination . "/" . $fileInfo["id"] . '_' . $fileInfo["name"] . ".manifest"
                        );
                    }
                } catch (\Exception $e) {
                    throw new InputOperationException(
                        "Failed to download file " . $fileInfo['name'] . ' ' . $fileInfo['id'],
                        0,
                        $e
                    );
                }
                $this->logger->info("Fetched file " . $fileInfo['name'] . " (" . $file["id"] . ").");
            }
        }
        $this->logger->info("All files were fetched.");
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
            $fileConfiguration["limit"] = 10;
        }
        $options->setLimit($fileConfiguration["limit"]);
        $files = $this->getClient()->listFiles($options);
        return $files;
    }

    /**
     * @param $fileInfo
     * @param $destination
     * @throws \Exception
     */
    protected function writeFileManifest($fileInfo, $destination)
    {
        $manifest = [
            "id" => $fileInfo["id"],
            "name" => $fileInfo["name"],
            "created" => $fileInfo["created"],
            "is_public" => $fileInfo["isPublic"],
            "is_encrypted" => $fileInfo["isEncrypted"],
            "is_sliced" => $fileInfo["isSliced"],
            "tags" => $fileInfo["tags"],
            "max_age_days" => $fileInfo["maxAgeDays"],
            "size_bytes" => $fileInfo["sizeBytes"]
        ];

        $adapter = new FileAdapter($this->getFormat());
        try {
            $adapter->setConfig($manifest);
            $adapter->writeToFile($destination);
        } catch (InvalidConfigurationException $e) {
            throw new InputOperationException(
                "Failed to write manifest for file {$fileInfo['id']} - {$fileInfo['name']}.",
                0,
                $e
            );
        }
    }

    /**
     * @param array $fileInfo array file info from Storage API
     * @param string $destination string Destination file path
     */
    protected function downloadFile($fileInfo, $destination)
    {
        // Initialize S3Client with credentials from Storage API
        $s3Client = new S3Client([
            "credentials" => [
                "key" => $fileInfo["credentials"]["AccessKeyId"],
                "secret" => $fileInfo["credentials"]["SecretAccessKey"],
                "token" => $fileInfo["credentials"]["SessionToken"]
            ],
            "region" => $fileInfo['region'],
            'version' => 'latest',

        ]);

        $fs = new Filesystem();
        if (!$fs->exists(dirname($destination))) {
            $fs->mkdir($destination);
        }

        // NonSliced file, just move from temp to destination file
        $s3Client->getObject([
            'Bucket' => $fileInfo["s3Path"]["bucket"],
            'Key'    => $fileInfo["s3Path"]["key"],
            'SaveAs' => $destination
        ]);
    }

    protected function downloadSlicedFile($fileInfo, $destination)
    {
        // Download manifest with all sliced files
        $client = new HttpClient([
            'handler' => HandlerStack::create([
                'backoffMaxTries' => 10,
            ]),
        ]);
        $manifest = json_decode($client->get($fileInfo['url'])->getBody());

        $part = 0;
        foreach ($manifest->entries as $slice) {
            $sliceInfo = $fileInfo;
            $sliceDestination = $destination . "/" . $fileInfo["id"] . '_' . $fileInfo["name"] . "." . $part++;

            $sliceInfo["s3Path"]["key"] = str_replace("s3://" . $fileInfo["s3Path"]["bucket"] . "/", "", $slice->url);
            $this->downloadFile($sliceInfo, $sliceDestination);
        }
    }

    /**
     * @param $configuration array list of input mappings
     * @param $destination string destination folder
     * @param string $storage
     */
    public function downloadTables($configuration, $destination, $storage = 'local')
    {
        if (!$configuration) {
            return;
        } elseif (!is_array($configuration)) {
            throw new InvalidInputException("Table export configuration is not an array.");
        }
        $tableExporter = new TableExporter($this->getClient());
        foreach ($configuration as $table) {
            if (!isset($table["destination"])) {
                $file = $destination . "/" . $table["source"];
            } else {
                $file = $destination . "/" . $table["destination"];
            }
            $exportOptions = ["format" => "rfc"];
            if (isset($table["columns"]) && count($table["columns"])) {
                $exportOptions["columns"] = $table["columns"];
            } else {
                $table["columns"] = [];
            }
            if (!empty($table["changed_since"]) && !empty($table["days"])) {
                throw new InvalidInputException("Cannot set both parameters 'days' and 'changed_since'.");
            }
            if (!empty($table["days"])) {
                $exportOptions["changedSince"] = "-{$table["days"]} days";
            }
            if (!empty($table["changed_since"])) {
                $exportOptions["changedSince"] = $table["changed_since"];
            }
            if (isset($table["where_column"]) && count($table["where_values"])) {
                $exportOptions["whereColumn"] = $table["where_column"];
                $exportOptions["whereValues"] = $table["where_values"];
                $exportOptions["whereOperator"] = $table["where_operator"];
            }
            if (isset($table['limit'])) {
                $exportOptions['limit'] = $table['limit'];
            }
            $this->logger->info("Fetching table " . $table["source"] . ".");
            $tableInfo = $this->getClient()->getTable($table["source"]);
            if ($storage == "s3") {
                $exportOptions['gzip'] = true;
                $job = $this->getClient()->exportTableAsync($table["source"], $exportOptions);
                $fileInfo = $this->getClient()->getFile(
                    $job["file"]["id"],
                    (new GetFileOptions())->setFederationToken(true)
                );
                $tableInfo["s3"] = $this->getS3Info($fileInfo);
            } elseif ($storage == "local") {
                $tableExporter->exportTable($table["source"], $file, $exportOptions);
            } else {
                throw new InvalidInputException("Parameter 'storage' must be either 'local' or 's3'.");
            }
            $this->logger->info("Fetched table " . $table["source"] . ".");

            $this->writeTableManifest($tableInfo, $file . ".manifest", $table["columns"]);
        }
        $this->logger->info("All tables were fetched.");
    }

    protected function getS3Info($fileInfo)
    {
        return [
            "isSliced" => $fileInfo["isSliced"],
            "region" => $fileInfo["region"],
            "bucket" => $fileInfo["s3Path"]["bucket"],
            "key" => $fileInfo["isSliced"]?$fileInfo["s3Path"]["key"] . "manifest":$fileInfo["s3Path"]["key"],
            "credentials" => [
                "access_key_id" => $fileInfo["credentials"]["AccessKeyId"],
                "secret_access_key" => $fileInfo["credentials"]["SecretAccessKey"],
                "session_token" => $fileInfo["credentials"]["SessionToken"]
            ]
        ];
    }

    /**
     * @param array $tableInfo
     * @param string $destination
     * @param array $columns
     */
    protected function writeTableManifest($tableInfo, $destination, $columns)
    {
        $manifest = [
            "id" => $tableInfo["id"],
            "uri" => $tableInfo["uri"],
            "name" => $tableInfo["name"],
            "primary_key" => $tableInfo["primaryKey"],
            "created" => $tableInfo["created"],
            "last_change_date" => $tableInfo["lastChangeDate"],
            "last_import_date" => $tableInfo["lastImportDate"],
        ];
        if (isset($tableInfo["s3"])) {
            $manifest["s3"] = $tableInfo["s3"];
        }
        if (!$columns) {
            $columns = $tableInfo["columns"];
        }
        $manifest["columns"] = $columns;

        $metadata = new Metadata($this->getClient());
        $manifest['metadata'] = $metadata->listTableMetadata($tableInfo['id']);
        $manifest['column_metadata'] = [];
        foreach ($columns as $column) {
            $manifest['column_metadata'][$column] = $metadata->listColumnMetadata($tableInfo['id'] . '.' . $column);
        }
        $adapter = new TableAdapter($this->getFormat());
        try {
            $adapter->setConfig($manifest);
            $adapter->writeToFile($destination);
        } catch (InvalidInputException $e) {
            throw new InputOperationException(
                "Failed to write manifest for table {$tableInfo['id']} - {$tableInfo['name']}.",
                $e
            );
        }
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
