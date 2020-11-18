<?php

namespace Keboola\OutputMapping\Tests\Writer;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use Symfony\Component\Filesystem\Filesystem;

abstract class BaseWriterTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var ClientWrapper
     */
    protected $clientWrapper;

    /**
     * @var Temp
     */
    protected $tmp;

    protected function clearBuckets($buckets)
    {
        foreach ($buckets as $bucket) {
            try {
                $this->clientWrapper->getBasicClient()->dropBucket($bucket, ['force' => true]);
            } catch (ClientException $e) {
                if ($e->getCode() != 404) {
                    throw $e;
                }
            }
        }
    }

    protected function clearFileUploads($tags)
    {
        // Delete file uploads
        $options = new ListFilesOptions();
        $options->setTags($tags);
        sleep(1);
        $files = $this->clientWrapper->getBasicClient()->listFiles($options);
        foreach ($files as $file) {
            $this->clientWrapper->getBasicClient()->deleteFile($file['id']);
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
        $this->initClient();
    }

    protected function initClient()
    {
        $this->clientWrapper = new ClientWrapper(
            new Client([
                'url' => STORAGE_API_URL,
                'token' => STORAGE_API_TOKEN,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]),
            null,
            null
        );
        $this->clientWrapper->setBranch('');
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
        $this->clearBuckets(['out.c-output-mapping-test']);
        $this->clearFileUploads(['output-mapping-test']);
        // Delete local files
        $this->tmp = null;
    }
}
