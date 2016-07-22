<?php

namespace Keboola\InputMapping\Reader;

use Aws\S3\S3Client;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter as FileAdapter;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter as TableAdapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\TableExporter;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;
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
     */
    public function __construct(Client $client)
    {
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
                if ($fileInfo['isSliced']) {
                    throw new InvalidInputException("File " . $file["id"] . " - " . $fileInfo["name"] . " is sliced and cannot be downloaded.");
                }
                try {
                    $this->downloadFile($fileInfo, $destination . "/" . $fileInfo["id"] . '_' . $fileInfo["name"]);
                    $this->writeFileManifest(
                        $fileInfo,
                        $destination . "/" . $fileInfo["id"] . '_' . $fileInfo["name"] . ".manifest"
                    );
                } catch (\Exception $e) {
                    throw new InputOperationException(
                        "Failed to download file " . $fileInfo['name'] . $fileInfo['id'],
                        $e
                    );
                }
            }
        }
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
        $files = $this->getClient()->listFiles($options);

        // a little sanity check, otherwise it may easily happen that a wrong ES query would fill up the server
        if (empty($fileConfiguration["limit"])) {
            $fileConfiguration["limit"] = 10;
        }
        if (count($files) > $fileConfiguration["limit"]) {
            throw new InvalidInputException(
                "File input mapping downloads more than $fileConfiguration[limit] files, this seems like a mistake."
            );
        }
        return $files;
    }

    /**
     * @param $fileInfo
     * @param $destination
     * @throws \Exception
     */
    protected function writeFileManifest($fileInfo, $destination)
    {
        $manifest = array(
            "id" => $fileInfo["id"],
            "name" => $fileInfo["name"],
            "created" => $fileInfo["created"],
            "is_public" => $fileInfo["isPublic"],
            "is_encrypted" => $fileInfo["isEncrypted"],
            "tags" => $fileInfo["tags"],
            "max_age_days" => $fileInfo["maxAgeDays"],
            "size_bytes" => $fileInfo["sizeBytes"]
        );

        $adapter = new FileAdapter($this->getFormat());
        try {
            $adapter->setConfig($manifest);
            $adapter->writeToFile($destination);
        } catch (InvalidConfigurationException $e) {
            throw new InputOperationException(
                "Failed to write manifest for file {$fileInfo['id']} - {$fileInfo['name']}.",
                $e
            );
        }
    }

    /**
     * @param $fileInfo array file info from Storage API
     * @param $destination string Destination file path
     */
    protected function downloadFile($fileInfo, $destination)
    {
        // Initialize S3Client with credentials from Storage API
        $s3Client = new S3Client(array(
            "credentials" => [
                "key" => $fileInfo["credentials"]["AccessKeyId"],
                "secret" => $fileInfo["credentials"]["SecretAccessKey"],
                "token" => $fileInfo["credentials"]["SessionToken"]
            ],
            "region" => 'us-east-1',
            'version' => 'latest',

        ));

        $fs = new Filesystem();
        if (!$fs->exists(dirname($destination))) {
            $fs->mkdir($destination);
        }

        /**
         * NonSliced file, just move from temp to destination file
         */
        $s3Client->getObject(array(
            'Bucket' => $fileInfo["s3Path"]["bucket"],
            'Key'    => $fileInfo["s3Path"]["key"],
            'SaveAs' => $destination
        ));
    }

    /**
     * @param $configuration array list of input mappings
     * @param $destination string destination folder
     */
    public function downloadTables($configuration, $destination)
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
            $exportOptions = array(
                "format" => "rfc"
            );
            if (isset($table["columns"]) && count($table["columns"])) {
                $exportOptions["columns"] = $table["columns"];
            }
            if (isset($table["days"])) {
                $exportOptions["changedSince"] = "-{$table["days"]} days";
            }
            if (isset($table["where_column"]) && count($table["where_values"])) {
                $exportOptions["whereColumn"] = $table["where_column"];
                $exportOptions["whereValues"] = $table["where_values"];
                $exportOptions["whereOperator"] = $table["where_operator"];
            }
            if (isset($table['limit'])) {
                $exportOptions['limit'] = $table['limit'];
            }
            $tableExporter->exportTable($table["source"], $file, $exportOptions);
            $tableInfo = $this->getClient()->getTable($table["source"]);
            $this->writeTableManifest($tableInfo, $file . ".manifest");
        }
    }

    /**
     * @param $tableInfo
     * @param $destination
     * @throws \Exception
     */
    protected function writeTableManifest($tableInfo, $destination)
    {
        $manifest = array(
            "id" => $tableInfo["id"],
            "uri" => $tableInfo["uri"],
            "name" => $tableInfo["name"],
            "primary_key" => $tableInfo["primaryKey"],
            "indexed_columns" => $tableInfo["indexedColumns"],
            "created" => $tableInfo["created"],
            "last_change_date" => $tableInfo["lastChangeDate"],
            "last_import_date" => $tableInfo["lastImportDate"],
            "rows_count" => $tableInfo["rowsCount"],
            "data_size_bytes" => $tableInfo["dataSizeBytes"],
            "is_alias" => $tableInfo["isAlias"],
            "columns" => $tableInfo["columns"],
            "attributes" => array()
        );
        foreach ($tableInfo["attributes"] as $attribute) {
            $manifest["attributes"][] = array(
                "name" => $attribute["name"],
                "value" => $attribute["value"],
                "protected" => $attribute["protected"]
            );
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
