<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\AbstractStrategyFactory;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use SplFileInfo;
use Symfony\Component\Finder\Finder;

class DownloadFilesRedshiftTest extends TestCase
{
    protected function getClientWrapper(?string $branchId): ClientWrapper
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
        string $tempDir
    ): StrategyFactory {
        $stagingFactory = new StrategyFactory($clientWrapper, new NullLogger(), 'json');
        $mockLocal = self::getMockBuilder(NullProvider::class)
            ->setMethods(['getPath'])
            ->getMock();
        $mockLocal->method('getPath')->willReturnCallback(
            function () use ($tempDir): string {
                return $tempDir;
            }
        );
        /** @var ProviderInterface $mockLocal */
        $stagingFactory->addProvider(
            $mockLocal,
            [
                AbstractStrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA]),
            ]
        );
        return $stagingFactory;
    }

    public function testReadSlicedFile(): void
    {
        $clientWrapper = $this->getClientWrapper(null);
        $temp = new Temp('input-mapping');
        $root = $temp->getTmpFolder();

        // Create bucket
        if (!$clientWrapper->getBasicClient()->bucketExists('in.c-docker-test-redshift')) {
            $clientWrapper->getBasicClient()->createBucket(
                'docker-test-redshift',
                Client::STAGE_IN,
                'Docker Testsuite',
                'redshift'
            );
        }

        // Create redshift table and export it to produce a sliced file
        if (!$clientWrapper->getBasicClient()->tableExists('in.c-docker-test-redshift.test_file')) {
            $csv = new CsvFile($root . '/upload.csv');
            $csv->writeRow(['Id', 'Name']);
            $csv->writeRow(['test', 'test']);
            $clientWrapper->getBasicClient()->createTableAsync('in.c-docker-test-redshift', 'test_file', $csv);
        }
        $table = $clientWrapper->getBasicClient()->exportTableAsync('in.c-docker-test-redshift.test_file');
        $fileId = $table['file']['id'];

        $reader = new Reader($this->getStagingFactory($clientWrapper, $root));
        $configuration = [['query' => 'id: ' . $fileId, 'overwrite' => true]];

        $dlDir = $root . '/download';
        $reader->downloadFiles(
            $configuration,
            '/download/',
            AbstractStrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        $fileName = $fileId . '_in.c-docker-test-redshift.test_file.csv';

        $resultFileContent = '';
        $finder = new Finder();

        /** @var SplFileInfo $file */
        foreach ($finder->files()->in($dlDir . '/' . $fileName) as $file) {
            $resultFileContent .= file_get_contents($file->getPathname());
        }

        self::assertEquals('"test","test"' . "\n", $resultFileContent);

        $manifestFile = $dlDir . '/' . $fileName . '.manifest';
        self::assertFileExists($manifestFile);
        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($manifestFile);
        self::assertArrayHasKey('is_sliced', $manifest);
        self::assertTrue($manifest['is_sliced']);
    }
}
