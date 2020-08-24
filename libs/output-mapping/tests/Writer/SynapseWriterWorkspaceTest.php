<?php

namespace Keboola\OutputMapping\Tests\Writer;

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
        BaseWriterTest::setUp();

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
        $this->tmp = new Temp();
        $this->tmp->initRunFolder();
        $root = $this->tmp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($root . DIRECTORY_SEPARATOR . 'upload');
        $fs->mkdir($root . DIRECTORY_SEPARATOR . 'download');

        $this->clearBuckets(['in.c-output-mapping-test', 'out.c-output-mapping-test']);
        $this->clearFileUploads(['docker-bundle-test']);
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
        $data = (array) $this->client->getTableDataPreview(
            'out.c-output-mapping-test.table1a',
            ['format' => 'json']
        );
        $values = [];
        foreach ($data['rows'] as $row) {
            foreach ($row as $column) {
                $values[] = $column['value'];
            }
        }
        sort($values);
        $this->assertEquals(['aabb', 'ccdd', 'test', 'test'], $values);
    }
}
