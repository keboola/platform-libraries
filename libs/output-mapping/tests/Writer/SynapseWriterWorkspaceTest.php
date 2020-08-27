<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\OutputMapping\Writer\TableWriter;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class SynapseWriterWorkspaceTest extends BaseWriterWorkspaceTest
{
    private $runSynapseTests = false;

    public function setUp()
    {
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
        parent::setUp();
    }

    protected function initClient()
    {
        $token = (string) getenv('SYNAPSE_STORAGE_API_TOKEN');
        $url = (string) getenv('SYNAPSE_STORAGE_API_URL');
        $this->client = new Client([
            "token" => $token,
            "url" => $url,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
        $tokenInfo = $this->client->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $this->client->getApiUrl()
        ));
    }

    public function testSynapseTableOutputMapping()
    {
        if (!$this->runSynapseTests) {
            $this->markTestSkipped('Synapse tests disabled');
        }
        $root = $this->tmp->getTmpFolder();
        $this->prepareWorkspaceWithTables('synapse');
        // snowflake bucket does not work - https://keboola.atlassian.net/browse/KBC-228
        $this->client->createBucket('output-mapping-test', 'out', '', 'synapse');
        $configs = [
            [
                'source' => 'table1a',
                'destination' => 'out.c-output-mapping-test.table1a',
            ],
            [
                'source' => 'table2a',
                'destination' => 'out.c-output-mapping-test.table2a',
            ],
        ];
        file_put_contents(
            $root . '/table1a.manifest',
            json_encode(
                ['columns' => ['Id', 'Name']]
            )
        );
        file_put_contents(
            $root . '/table2a.manifest',
            json_encode(
                ['columns' => ['Id2', 'Name2']]
            )
        );
        $writer = new TableWriter($this->client, new NullLogger(), $this->getWorkspaceProvider());

        $tableQueue = $writer->uploadTables(
            $root,
            ['mapping' => $configs],
            ['componentId' => 'foo'],
            'workspace-synapse'
        );
        $jobIds = $tableQueue->waitForAll();
        $this->assertCount(2, $jobIds);

        $tables = $this->client->listTables('out.c-output-mapping-test');
        $this->assertCount(2, $tables);
        $tableIds = [$tables[0]['id'], $tables[1]['id']];
        sort($tableIds);
        $this->assertEquals(['out.c-output-mapping-test.table1a', 'out.c-output-mapping-test.table2a'], $tableIds);
        $this->assertCount(2, $jobIds);
        $this->assertNotEmpty($jobIds[0]);
        $this->assertNotEmpty($jobIds[1]);

        $reader = new Reader($this->client, new NullLogger(), $this->getWorkspaceProvider());
        $reader->downloadTables(
            new InputTableOptionsList([
                [
                    'source' => 'out.c-output-mapping-test.table1a',
                    'destination' => 'table1a-returned.csv',
                ]
            ]),
            new InputTableStateList([]),
            $root
        );
        $expectedCsvOutput = "\"Id\",\"Name\"\n\"aabb\",\"ccdd\"\n\"test\",\"test\"\n";
        self::assertEquals(
            $expectedCsvOutput,
            (string) file_get_contents($root . DIRECTORY_SEPARATOR . 'table1a-returned.csv')
        );
    }
}