<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\StorageApi\Client;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class ReaderTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var Temp
     */
    private $temp;

    public function setUp()
    {
        // Create folders
        $this->temp = new Temp('docker');
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/download');
        $this->client = new Client(['token' => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]);
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

    public function tearDown()
    {
        $this->temp = null;
    }

    public function testParentId()
    {
        $reader = new Reader($this->client, new NullLogger(), new NullWorkspaceProvider());
        $this->client->setRunId('123456789');
        self::assertEquals('123456789', $reader->getParentRunId());
        $this->client->setRunId('123456789.98765432');
        self::assertEquals('123456789', $reader->getParentRunId());
        $this->client->setRunId('123456789.98765432.4563456');
        self::assertEquals('123456789.98765432', $reader->getParentRunId());
        $this->client->setRunId(null);
        self::assertEquals('', $reader->getParentRunId());
    }

    public function testReadInvalidConfiguration1()
    {
        // empty configuration, ignored
        $reader = new Reader($this->client, new NullLogger(), new NullWorkspaceProvider());
        $configuration = null;
        $reader->downloadFiles($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadInvalidConfiguration2()
    {
        // empty configuration, ignored
        $reader = new Reader($this->client, new NullLogger(), new NullWorkspaceProvider());
        $configuration = 'foobar';
        try {
            /** @noinspection PhpParamsInspection */
            $reader->downloadFiles($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
            self::fail('Invalid configuration should fail.');
        } catch (InvalidInputException $e) {
            self::assertContains('File download configuration is not an array', $e->getMessage());
        }
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadInvalidConfiguration3()
    {
        // empty configuration, ignored
        $reader = new Reader($this->client, new NullLogger(), new NullWorkspaceProvider());
        $configuration = new InputTableOptionsList([]);
        $reader->downloadTables($configuration, new InputTableStateList([]), $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadTablesDefaultBackend()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->client, $logger, new NullWorkspaceProvider());
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-docker-test.test',
                'destination' => 'test.csv'
            ],
            [
                'source' => 'in.c-docker-test.test2',
                'destination' => 'test2.csv'
            ]
        ]);

        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage(
            'Parameter "storage" must be one of: s3, abs, local, workspace-redshift, workspace-snowflake'
        );
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            'invalid'
        );
    }
}
