<?php

namespace Keboola\InputMapping\Tests;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Options\InputTablesOptions;
use Keboola\InputMapping\Reader\Reader;
use Keboola\StorageApi\Client;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
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
    }

    public function tearDown()
    {
        $this->temp = null;
    }

    public function testParentId()
    {
        $reader = new Reader($this->client, new NullLogger());
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
        $reader = new Reader($this->client, new NullLogger());
        $configuration = null;
        $reader->downloadFiles($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadInvalidConfiguration2()
    {
        // empty configuration, ignored
        $reader = new Reader($this->client, new NullLogger());
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
        $reader = new Reader($this->client, new NullLogger());
        $configuration = new InputTablesOptions([]);
        $reader->downloadTables($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }
}
