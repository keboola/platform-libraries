<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\InputTableOptionsList;
use Keboola\InputMapping\Table\Options\ReaderOptions;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class ReaderTest extends TestCase
{
    private Temp $temp;

    public function setUp(): void
    {
        // Create folders
        $this->temp = new Temp('docker');
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/download');
        $clientWrapper = $this->getClientWrapper(null);
        $tokenInfo = $clientWrapper->getBasicClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $clientWrapper->getBasicClient()->getApiUrl()
        ));
    }

    private function getClientWrapper(?string $branchId): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN_MASTER'),
                $branchId
            ),
        );
    }

    protected function getStagingFactory(
        ClientWrapper $clientWrapper,
        string $format = 'json',
        ?LoggerInterface $logger = null
    ): StrategyFactory {
        $stagingFactory = new StrategyFactory(
            $clientWrapper,
            $logger ?: new NullLogger(),
            $format
        );
        $mockLocal = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getPath'])
            ->getMock();
        $mockLocal->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            }
        );
        $stagingFactory->addProvider(
            $mockLocal,
            [
                AbstractStrategyFactory::LOCAL => new Scope([
                    Scope::TABLE_DATA, Scope::TABLE_METADATA,
                    Scope::FILE_DATA, Scope::FILE_METADATA,
                ]),
            ]
        );
        return $stagingFactory;
    }

    public function testParentId(): void
    {
        $clientWrapper = $this->getClientWrapper(null);
        $clientWrapper->getBasicClient()->setRunId('123456789');
        self::assertEquals(
            '123456789',
            Reader::getParentRunId((string) $clientWrapper->getBasicClient()->getRunId())
        );
        $clientWrapper->getBasicClient()->setRunId('123456789.98765432');
        self::assertEquals(
            '123456789',
            Reader::getParentRunId((string) $clientWrapper->getBasicClient()->getRunId())
        );
        $clientWrapper->getBasicClient()->setRunId('123456789.98765432.4563456');
        self::assertEquals(
            '123456789.98765432',
            Reader::getParentRunId((string) $clientWrapper->getBasicClient()->getRunId())
        );
        $clientWrapper->getBasicClient()->setRunId(null);
        self::assertEquals(
            '',
            Reader::getParentRunId((string) $clientWrapper->getBasicClient()->getRunId())
        );
    }

    public function testReadInvalidConfiguration(): void
    {
        // empty configuration, ignored
        $reader = new Reader($this->getStagingFactory($this->getClientWrapper(null)));
        $configuration = new InputTableOptionsList([]);
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true)
        );
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadInvalidConfigurationNoQueryNoTagsNoSource(): void
    {
        $reader = new Reader($this->getStagingFactory($this->getClientWrapper(null)));
        $configurations = [[]];
        try {
            $reader->downloadFiles(
                $configurations,
                'download',
                AbstractStrategyFactory::LOCAL,
                new InputFileStateList([])
            );
            self::fail('Invalid configuration should fail.');
        } catch (InvalidInputException $e) {
            self::assertStringContainsString(
                "Invalid file mapping, 'tags', 'query' and 'source.tags' are all empty.",
                $e->getMessage()
            );
        }
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadInvalidConfigurationBothTagsAndSourceTags(): void
    {
        $reader = new Reader($this->getStagingFactory($this->getClientWrapper(null)));
        $configurations = [
            [
                'source' => [
                    'tags' => [
                        [
                            'name' => 'tag',
                        ],
                    ],
                ],
                'tags' => [
                    'tag',
                ],
            ],
        ];
        try {
            $reader->downloadFiles(
                $configurations,
                'download',
                AbstractStrategyFactory::LOCAL,
                new InputFileStateList([])
            );
            self::fail('Invalid configuration should fail.');
        } catch (InvalidInputException $e) {
            self::assertStringContainsString(
                'Invalid file mapping, both \'tags\' and \'source.tags\' cannot be set.',
                $e->getMessage()
            );
        }
        $finder = new Finder();
        $files = $finder->files()->in($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download');
        self::assertEmpty($files);
    }

    public function testReadTablesDefaultBackend(): void
    {
        $reader = new Reader($this->getStagingFactory($this->getClientWrapper(null)));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-docker-test.test',
                'destination' => 'test.csv',
            ],
            [
                'source' => 'in.c-docker-test.test2',
                'destination' => 'test2.csv',
            ],
        ]);

        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(
            'Input mapping on type "invalid" is not supported. Supported types are "abs, local, s3, workspace-abs,'
        );
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            'download',
            'invalid',
            new ReaderOptions(true)
        );
    }

    public function testReadTablesDefaultBackendBranchRewrite(): void
    {
        $temp = new Temp(uniqid('input-mapping'));
        file_put_contents($temp->getTmpFolder() . 'data.csv', "foo,bar\n1,2");
        $csvFile = new CsvFile($temp->getTmpFolder() . 'data.csv');

        $clientWrapper = $this->getClientWrapper(null);
        try {
            $clientWrapper->getBasicClient()->dropBucket('in.c-my-branch-docker-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        try {
            $clientWrapper->getBasicClient()->dropBucket('in.c-docker-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
        foreach ($clientWrapper->getBasicClient()->listBuckets() as $bucket) {
            if (preg_match('/^c-[0-9]+-docker-test$/ui', $bucket['name'])) {
                $clientWrapper->getBasicClient()->dropBucket($bucket['id'], ['force' => true]);
            }
        }
        $branchesApi = new DevBranches($clientWrapper->getBasicClient());
        foreach ($branchesApi->listBranches() as $branch) {
            if ($branch['name'] === 'my-branch') {
                $branchesApi->deleteBranch($branch['id']);
            }
        }
        $branchId = (string) $branchesApi->createBranch('my-branch')['id'];

        $branchBucketId = $clientWrapper->getBasicClient()->createBucket(
            sprintf('%s-docker-test', $branchId),
            'in'
        );
        $clientWrapper->getBasicClient()->createTable($branchBucketId, 'test', $csvFile);
        $reader = new Reader($this->getStagingFactory($this->getClientWrapper($branchId)));
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-docker-test.test',
                'destination' => 'test.csv',
            ],
        ]);
        $state = new InputTableStateList([
            [
                'source' => 'in.c-docker-test.test',
                'lastImportDate' => '1605741600',
            ],
        ]);
        $result = $reader->downloadTables(
            $configuration,
            $state,
            'download',
            AbstractStrategyFactory::LOCAL,
            new ReaderOptions(true)
        );
        self::assertStringContainsString(
            "\"foo\",\"bar\"\n\"1\",\"2\"",
            (string) file_get_contents($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download/test.csv')
        );
        $data = $result->getInputTableStateList()->jsonSerialize();
        self::assertEquals(sprintf('%s.test', $branchBucketId), $data[0]['source']);
        self::assertArrayHasKey('lastImportDate', $data[0]);
    }
}
