<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\NullLogger;

class DownloadTablesSynapseTest extends DownloadTablesTestAbstract
{
    private $runSynapseTests;

    public function setUp()
    {
        parent::setUp();
        $this->runSynapseTests = getenv('RUN_SYNAPSE_TESTS');
        if (!$this->runSynapseTests) {
            return;
        }
        if (getenv('SYNAPSE_STORAGE_API_TOKEN') === false) {
            throw new Exception('SYNAPSE_STORAGE_API_TOKEN must be set for synapse tests');
        }
        if (getenv('SYNAPSE_STORAGE_API_URL') === false) {
            throw new Exception('SYNAPSE_STORAGE_API_URL must be set for synapse tests');
        }
        try {
            $this->clientWrapper->getBasicClient()->dropBucket("in.c-docker-test-synapse", ["force" => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket(
            "docker-test-synapse",
            Client::STAGE_IN,
            "Docker Testsuite",
            "synapse"
        );

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . "upload.csv");
        $csv->writeRow(["Id", "Name"]);
        $csv->writeRow(["test", "test"]);
        $this->clientWrapper->getBasicClient()->createTableAsync("in.c-docker-test-synapse", "test", $csv);
    }

    protected function initClient()
    {
        $token = (string) getenv('SYNAPSE_STORAGE_API_TOKEN');
        $url = (string) getenv('SYNAPSE_STORAGE_API_URL');
        $this->clientWrapper = new ClientWrapper(
            new Client(["token" => $token, "url" => $url]),
            null,
            null
        );
        $this->clientWrapper->setBranch('');
        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $this->clientWrapper->getBasicClient()->getApiUrl()
        ));
    }

    public function testReadTablesSynapse()
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-docker-test-synapse.test",
                "destination" => "test-synapse.csv"
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            'local'
        );

        self::assertEquals(
            "\"Id\",\"Name\"\n\"test\",\"test\"\n",
            file_get_contents($this->temp->getTmpFolder(). "/download/test-synapse.csv")
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test-synapse.csv.manifest");
        self::assertEquals("in.c-docker-test-synapse.test", $manifest["id"]);
    }

    public function testReadTablesABSSynapse()
    {
        if (!$this->runSynapseTests) {
            self::markTestSkipped('Synapse tests disabled');
        }
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-docker-test-synapse.test",
                "destination" => "test-synapse.csv"
            ]
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            'abs'
        );
        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/test-synapse.csv.manifest");
        self::assertEquals("in.c-docker-test-synapse.test", $manifest["id"]);
        $this->assertABSinfo($manifest);
    }

    public function testReadTablesEmptySlices()
    {
        if (!$this->runSynapseTests) {
            $this->markTestSkipped('Synapse tests disabled');
        }
        $fileUploadOptions = new FileUploadOptions();
        $fileUploadOptions
            ->setIsSliced(true)
            ->setFileName('emptyfile');
        $uploadFileId = $this->clientWrapper->getBasicClient()->uploadSlicedFile([], $fileUploadOptions);
        $columns = ['Id', 'Name'];
        $headerCsvFile = new CsvFile($this->temp->getTmpFolder() . 'header.csv');
        $headerCsvFile->writeRow($columns);
        $this->clientWrapper->getBasicClient()->createTableAsync('in.c-docker-test-synapse', 'empty', $headerCsvFile, []);

        $options['columns'] = $columns;
        $options['dataFileId'] = $uploadFileId;
        $this->clientWrapper->getBasicClient()->writeTableAsyncDirect('in.c-docker-test-synapse.empty', $options);

        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = new InputTableOptionsList([
            [
                "source" => "in.c-docker-test-synapse.empty",
                "destination" => "empty.csv",
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download'
        );
        $file = file_get_contents($this->temp->getTmpFolder() . "/download/empty.csv");
        self::assertEquals("\"Id\",\"Name\"\n", $file);

        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . "/download/empty.csv.manifest");
        self::assertEquals("in.c-docker-test-synapse.empty", $manifest["id"]);
    }
}
