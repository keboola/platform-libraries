<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class DownloadFilesTestAbstract extends TestCase
{
    const TEST_FILE_TAG_FOR_BRANCH = 'testReadFilesForBranch';
    const DEFAULT_TEST_FILE_TAG = 'download-files-test';

    /** @var ClientWrapper */
    protected $clientWrapper;

    /** @var string */
    protected $tmpDir;

    /** @var Temp */
    protected $temp;

    public function setUp()
    {
        // Create folders
        $temp = new Temp('docker');
        $temp->initRunFolder();
        $this->temp = $temp;
        $this->tmpDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($this->tmpDir . "/download");

        $this->initClient();

        // Delete file uploads
        sleep(5);
        $options = new ListFilesOptions();
        $options->setTags([self::DEFAULT_TEST_FILE_TAG, self::TEST_FILE_TAG_FOR_BRANCH]);
        $options->setLimit(1000);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        foreach ($files as $file) {
            $this->clientWrapper->getBasicClient()->deleteFile($file["id"]);
        }
    }

    protected function initClient()
    {
        $this->clientWrapper = new ClientWrapper(
            new Client(["token" => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]),
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

    protected function getStagingFactory($clientWrapper = null, $format = 'json', $logger = null)
    {
        $stagingFactory = new StrategyFactory(
            $clientWrapper ? $clientWrapper : $this->clientWrapper,
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
