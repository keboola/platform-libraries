<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use Symfony\Component\Filesystem\Filesystem;

class DownloadFilesTestAbstract extends \PHPUnit_Framework_TestCase
{
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
        $options->setTags(["download-files-test"]);
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
}
