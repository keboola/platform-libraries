<?php

namespace Keboola\InputMapping\Tests\Functional;

use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\InputMapping\Staging\Scope;
use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\StorageApi\Client;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class DownloadTablesTestAbstract extends TestCase
{
    /** @var ClientWrapper */
    protected $clientWrapper;

    /** @var Temp */
    protected $temp;

    public function setUp()
    {
        parent::setUp();
        $this->temp = new Temp('docker');
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . "/download");
        $this->initClient();
    }

    protected function initClient()
    {
        $this->clientWrapper = new ClientWrapper(
            new Client(["token" => STORAGE_API_TOKEN, "url" => STORAGE_API_URL]),
            null,
            null
        );
        $this->clientWrapper->setBranchId('');
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

    /**
     * @param array $manifest
     */
    protected function assertABSinfo(array $manifest)
    {
        self::assertArrayHasKey("abs", $manifest);
        self::assertArrayHasKey("is_sliced", $manifest["abs"]);
        self::assertArrayHasKey("region", $manifest["abs"]);
        self::assertArrayHasKey("container", $manifest["abs"]);
        self::assertArrayHasKey("name", $manifest["abs"]);
        self::assertArrayHasKey("credentials", $manifest["abs"]);
        self::assertArrayHasKey("sas_connection_string", $manifest["abs"]['credentials']);
        self::assertArrayHasKey("expiration", $manifest["abs"]['credentials']);

        if ($manifest["abs"]["is_sliced"]) {
            self::assertStringEndsWith("manifest", $manifest["abs"]["name"]);
        }
    }

    /**
     * @param $expectedString
     * @param $path
     */
    public static function assertCSVEquals($expectedString, $path)
    {
        $expectedArray = explode("\n", $expectedString);
        $actualArray = explode("\n", file_get_contents($path));

        // compare length
        self::assertEquals(count($expectedArray), count($actualArray));
        // compare headers
        self::assertEquals($expectedArray[0], $actualArray[0]);

        $actualArrayWithoutHeader = array_slice($actualArray, 1);
        // compare each line
        for ($i = 1; $i < count($expectedArray); $i++) {
            self::assertTrue(in_array($expectedArray[$i], $actualArrayWithoutHeader));
        }
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
                StrategyFactory::LOCAL => new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA]),
                StrategyFactory::ABS => new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA]),
                StrategyFactory::S3 => new Scope([Scope::TABLE_DATA, Scope::TABLE_METADATA]),
            ]
        );
        return $stagingFactory;
    }
}
