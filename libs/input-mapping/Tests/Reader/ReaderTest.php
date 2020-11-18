<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\NullWorkspaceProvider;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class ReaderTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var ClientWrapper
     */
    protected $clientWrapper;

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
        $this->clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]),
            null,
            null
        );
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

    public function tearDown()
    {
        $this->temp = null;
    }

    public function testParentId()
    {
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $this->clientWrapper->getBasicClient()->setRunId('123456789');
        self::assertEquals('123456789', $reader->getParentRunId());
        $this->clientWrapper->getBasicClient()->setRunId('123456789.98765432');
        self::assertEquals('123456789', $reader->getParentRunId());
        $this->clientWrapper->getBasicClient()->setRunId('123456789.98765432.4563456');
        self::assertEquals('123456789.98765432', $reader->getParentRunId());
        $this->clientWrapper->getBasicClient()->setRunId(null);
        self::assertEquals('', $reader->getParentRunId());
    }

    public function testReadInvalidConfiguration1()
    {
        // empty configuration, ignored
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = null;
        $reader->downloadFiles($configuration, $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadInvalidConfiguration2()
    {
        // empty configuration, ignored
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
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
        $this->clientWrapper->setBranch('');
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = new InputTableOptionsList([]);
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download'
        );
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadTablesDefaultBackend()
    {
        $logger = new TestLogger();
        $this->clientWrapper->setBranch('');
        $reader = new Reader($this->clientWrapper, $logger, new NullWorkspaceProvider());
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

    public function testReadTablesDefaultBackendBranchRewrite()
    {
        $logger = new TestLogger();
        $temp = new Temp(uniqid('input-mapping'));
        $temp->initRunFolder();
        file_put_contents($temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($temp->getTmpFolder() . 'data.csv');
        try {
            $this->clientWrapper->getBasicClient()->dropBucket('in.c-my-branch-docker-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        try {
            $this->clientWrapper->getBasicClient()->dropBucket('in.c-docker-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket('my-branch-docker-test', 'in');
        $this->clientWrapper->getBasicClient()->createTable('in.c-my-branch-docker-test', 'test', $csvFile);
        $this->clientWrapper->setBranch('my-branch');
        $reader = new Reader($this->clientWrapper, $logger, new NullWorkspaceProvider());
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-docker-test.test',
                'destination' => 'test.csv'
            ],
        ]);
        $state = new InputTableStateList([
            [
                'source' => 'in.c-docker-test.test',
                'lastImportDate' => '1605741600',
            ],
        ]);
        $outState = $reader->downloadTables(
            $configuration,
            $state,
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            'local'
        );
        self::assertContains(
            "\"foo\",\"bar\"\n\"1\",\"2\"",
            file_get_contents($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test.csv')
        );
        $data = $outState->jsonSerialize();
        self::assertEquals('in.c-my-branch-docker-test.test', $data[0]['source']);
        self::assertArrayHasKey('lastImportDate', $data[0]);
    }
}
