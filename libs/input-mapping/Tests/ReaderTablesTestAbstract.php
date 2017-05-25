<?php

namespace Keboola\InputMapping\Tests;

use Keboola\StorageApi\Client;
use Keboola\Temp\Temp;
use Symfony\Component\Filesystem\Filesystem;

class ReaderTablesTestAbstract extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var Temp
     */
    protected $temp;

    /**
     * @param array $manifest
     */
    protected function assertS3info(array $manifest)
    {
        self::assertArrayHasKey("s3", $manifest);
        self::assertArrayHasKey("isSliced", $manifest["s3"]);
        self::assertArrayHasKey("region", $manifest["s3"]);
        self::assertArrayHasKey("bucket", $manifest["s3"]);
        self::assertArrayHasKey("key", $manifest["s3"]);
        self::assertArrayHasKey("credentials", $manifest["s3"]);
        self::assertArrayHasKey("access_key_id", $manifest["s3"]["credentials"]);
        self::assertArrayHasKey("secret_access_key", $manifest["s3"]["credentials"]);
        self::assertArrayHasKey("session_token", $manifest["s3"]["credentials"]);
        self::assertContains("gz", $manifest["s3"]["key"]);

        if ($manifest["s3"]["isSliced"]) {
            self::assertContains("manifest", $manifest["s3"]["key"]);
        }
    }

    public function setUp()
    {
        parent::setUp();
        $this->temp = new Temp('docker');
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . "/download");
        $this->client = new Client(["token" => STORAGE_API_TOKEN]);
    }
}
