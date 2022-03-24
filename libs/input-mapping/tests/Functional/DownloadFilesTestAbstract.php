<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class DownloadFilesTestAbstract extends TestCase
{
    const TEST_FILE_TAG_FOR_BRANCH = 'testReadFilesForBranch';
    const DEFAULT_TEST_FILE_TAG = 'download-files-test';

    protected string $tmpDir;
    protected Temp $temp;

    public function setUp()
    {
        // Create folders
        $temp = new Temp('docker');
        $temp->initRunFolder();
        $this->temp = $temp;
        $this->tmpDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($this->tmpDir . "/download");

        // Delete file uploads
        sleep(5);
        $options = new ListFilesOptions();
        $options->setTags([self::DEFAULT_TEST_FILE_TAG, self::TEST_FILE_TAG_FOR_BRANCH]);
        $options->setLimit(1000);
        $clientWrapper = $this->getClientWrapper(null);
        $files = $clientWrapper->getBasicClient()->listFiles($options);
        foreach ($files as $file) {
            $clientWrapper->getBasicClient()->deleteFile($file["id"]);
        }
    }

    protected function getClientWrapper(?string $branchId): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN_MASTER, $branchId),
        );
    }

    protected function getStagingFactory($clientWrapper, $format = 'json', $logger = null): StrategyFactory
    {
        $stagingFactory = new StrategyFactory(
            $clientWrapper,
            $logger ? $logger : new NullLogger(),
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
        /** @var ProviderInterface $mockLocal */
        $stagingFactory->addProvider(
            $mockLocal,
            [
                StrategyFactory::LOCAL => new Scope([Scope::FILE_DATA, Scope::FILE_METADATA])
            ]
        );
        return $stagingFactory;
    }
}
