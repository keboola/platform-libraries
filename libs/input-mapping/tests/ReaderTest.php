<?php

namespace Keboola\InputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\NullWorkspaceProvider;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
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
        self::assertEquals(
            '123456789',
            Reader::getParentRunId($this->clientWrapper->getBasicClient()->getRunId())
        );
        $this->clientWrapper->getBasicClient()->setRunId('123456789.98765432');
        self::assertEquals(
            '123456789',
            Reader::getParentRunId($this->clientWrapper->getBasicClient()->getRunId())
        );
        $this->clientWrapper->getBasicClient()->setRunId('123456789.98765432.4563456');
        self::assertEquals(
            '123456789.98765432',
            Reader::getParentRunId($this->clientWrapper->getBasicClient()->getRunId())
        );
        $this->clientWrapper->getBasicClient()->setRunId(null);
        self::assertEquals(
            '',
            Reader::getParentRunId($this->clientWrapper->getBasicClient()->getRunId())
        );
    }

    public function testReadInvalidConfiguration1()
    {
        // empty configuration, ignored
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = null;
        $reader->downloadFiles(
            $configuration,
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            Reader::STAGING_LOCAL
        );
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
            $reader->downloadFiles(
                $configuration,
                $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
                Reader::STAGING_LOCAL
            );
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
        $this->clientWrapper->setBranchId('');
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configuration = new InputTableOptionsList([]);
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            Reader::STAGING_LOCAL
        );
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadInvalidConfigurationNoQueryNoTagsNoSource()
    {
        $this->clientWrapper->setBranchId('');
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configurations = [[]];
        try {
            /** @noinspection PhpParamsInspection */
            $reader->downloadFiles(
                $configurations,
                $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
                Reader::STAGING_LOCAL
            );
            self::fail('Invalid configuration should fail.');
        } catch (InvalidInputException $e) {
            self::assertContains("Invalid file mapping, 'tags', 'query' and 'source.tags' are empty.", $e->getMessage());
        }
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadInvalidConfigurationBothTagsAndSourceTags()
    {
        $this->clientWrapper->setBranchId('');
        $reader = new Reader($this->clientWrapper, new NullLogger(), new NullWorkspaceProvider());
        $configurations = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => 'tag'
                        ]
                    ]
                ],
                'tags' => [
                    'tag'
                ]
            ]
        ];
        try {
            /** @noinspection PhpParamsInspection */
            $reader->downloadFiles(
                $configurations,
                $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
                Reader::STAGING_LOCAL
            );
            self::fail('Invalid configuration should fail.');
        } catch (InvalidInputException $e) {
            self::assertContains("Invalid file mapping, both 'tags' and 'source.tags' cannot be set.", $e->getMessage());
        }
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadTablesDefaultBackend()
    {
        $logger = new TestLogger();
        $this->clientWrapper->setBranchId('');
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

        $this->clientWrapper = new ClientWrapper(
            new Client(['token' => STORAGE_API_TOKEN_MASTER, "url" => STORAGE_API_URL]),
            null,
            null
        );
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
        foreach ($this->clientWrapper->getBasicClient()->listBuckets() as $bucket) {
            if (preg_match('/^c\-[0-9]+\-docker\-test$/ui', $bucket['name'])) {
                $this->clientWrapper->getBasicClient()->dropBucket($bucket['id'], ['force' => true]);
            }
        }
        $branches = new DevBranches($this->clientWrapper->getBasicClient());
        foreach ($branches->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branches->deleteBranch($branch['id']);
            }
        }
        $branchId = $branches->createBranch('my-branch')['id'];

        $branchBucketId = $this->clientWrapper->getBasicClient()->createBucket(sprintf('%s-docker-test', $branchId), 'in');
        $this->clientWrapper->getBasicClient()->createTable($branchBucketId, 'test', $csvFile);
        $this->clientWrapper->setBranchId($branchId);
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
        self::assertEquals(sprintf('%s.test', $branchBucketId), $data[0]['source']);
        self::assertArrayHasKey('lastImportDate', $data[0]);
    }
}
