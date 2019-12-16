<?php


namespace Keboola\OutputMapping\Tests\Writer;


use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\Temp\Temp;
use Symfony\Component\Filesystem\Filesystem;

abstract class BaseWriterTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var Temp
     */
    protected $tmp;

    protected function clearBucket()
    {
        foreach (['out.c-docker-test'] as $bucket) {
            try {
                $this->client->dropBucket($bucket, ['force' => true]);
            } catch (ClientException $e) {
                if ($e->getCode() != 404) {
                    throw $e;
                }
            }
        }
    }

    protected function clearFileUploads()
    {
        // Delete file uploads
        $options = new ListFilesOptions();
        $options->setTags(['docker-bundle-test']);
        sleep(1);
        $files = $this->client->listFiles($options);
        foreach ($files as $file) {
            $this->client->deleteFile($file['id']);
        }
    }

    public function setUp()
    {
        parent::setUp();
        $this->tmp = new Temp();
        $this->tmp->initRunFolder();
        $root = $this->tmp->getTmpFolder();
        $fs = new Filesystem();
        $fs->mkdir($root . DIRECTORY_SEPARATOR . 'upload');
        $fs->mkdir($root . DIRECTORY_SEPARATOR . 'download');

        $this->client = new Client([
            'url' => STORAGE_API_URL,
            'token' => STORAGE_API_TOKEN,
            'backoffMaxTries' => 1,
            'jobPollRetryDelay' => function () {
                return 1;
            },
        ]);
        $this->clearBucket();
        $this->clearFileUploads();
    }

    public function tearDown()
    {
        $this->clearBucket();
        $this->clearFileUploads();
        // Delete local files
        $this->tmp = null;
    }
}