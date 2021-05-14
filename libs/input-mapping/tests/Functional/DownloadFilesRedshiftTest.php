<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Reader;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\StorageApi\Client;
use Symfony\Component\Finder\Finder;

class DownloadFilesRedshiftTest extends DownloadFilesTestAbstract
{
    public function testReadSlicedFile()
    {
        $this->clientWrapper->setBranchId('');

        // Create bucket
        if (!$this->clientWrapper->getBasicClient()->bucketExists("in.c-docker-test-redshift")) {
            $this->clientWrapper->getBasicClient()->createBucket(
                "docker-test-redshift",
                Client::STAGE_IN,
                "Docker Testsuite",
                "redshift"
            );
        }

        // Create redshift table and export it to produce a sliced file
        if (!$this->clientWrapper->getBasicClient()->tableExists("in.c-docker-test-redshift.test_file")) {
            $csv = new CsvFile($this->tmpDir . "/upload.csv");
            $csv->writeRow(["Id", "Name"]);
            $csv->writeRow(["test", "test"]);
            $this->clientWrapper->getBasicClient()->createTableAsync("in.c-docker-test-redshift", "test_file", $csv);
        }
        $table = $this->clientWrapper->getBasicClient()->exportTableAsync('in.c-docker-test-redshift.test_file');
        $fileId = $table['file']['id'];

        $reader = new Reader($this->getStagingFactory());
        $configuration = [['query' => 'id: ' . $fileId, 'overwrite' => true]];

        $dlDir = $this->tmpDir . '/download';
        $reader->downloadFiles(
            $configuration,
            '/download/',
            StrategyFactory::LOCAL,
            new InputFileStateList([])
        );
        $fileName = $fileId . "_in.c-docker-test-redshift.test_file.csv";

        $resultFileContent = '';
        $finder = new Finder();

        /** @var \SplFileInfo $file */
        foreach ($finder->files()->in($dlDir . '/' . $fileName) as $file) {
            $resultFileContent .= file_get_contents($file->getPathname());
        }

        self::assertEquals('"test","test"' . "\n", $resultFileContent);

        $manifestFile = $dlDir . "/" . $fileName . ".manifest";
        self::assertFileExists($manifestFile);
        $adapter = new Adapter();
        $manifest = $adapter->readFromFile($manifestFile);
        self::assertArrayHasKey('is_sliced', $manifest);
        self::assertTrue($manifest['is_sliced']);
    }
}
