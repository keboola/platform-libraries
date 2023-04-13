<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\File\Strategy;

use Keboola\InputMapping\Staging\NullProvider;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Writer\File\Strategy\Local;
use Keboola\StorageApi\ClientException;
use Psr\Log\Test\TestLogger;
use stdClass;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Yaml\Yaml;

class LocalTest extends AbstractTestCase
{
    public function setUp(): void
    {
        parent::setUp();
    }

    private function getProvider(): ProviderInterface
    {
        $mockLocal = self::createMock(NullProvider::class);
        $mockLocal->method('getPath')->willReturnCallback(
            function () {
                return $this->temp->getTmpFolder();
            }
        );
        return $mockLocal;
    }

    public function testListFilesNoFiles(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'json'
        );
        $files = $strategy->listFiles('');
        self::assertSame([], $files);
    }

    public function testListFilesNonExistentDir(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'json'
        );
        self::expectException(OutputOperationException::class);
        self::expectExceptionMessage('non-existent-directory/and-file" directory does not exist.".');
        $strategy->listFiles('non-existent-directory/and-file');
    }

    public function testListFiles(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'json'
        );
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/data/out/files');
        $fs->mkdir($this->temp->getTmpFolder() . '/data/out/tables');
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-file',
            'my-contents'
        );
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-file.manifest',
            'manifest data'
        );
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-second-file',
            'second file'
        );
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-second-file.manifest',
            '2nd manifest data'
        );
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/tables/my-file',
            'my-contents'
        );
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/tables/my-file.manifest',
            'table manifest'
        );
        $files = $strategy->listFiles('/data/out/files');
        $fileNames = [];
        foreach ($files as $file) {
            $fileNames[$file->getName()] = $file->getPath();
        }
        $keys = array_keys($fileNames);
        sort($keys);
        self::assertEquals(['my-file', 'my-second-file'], $keys);
        self::assertStringEndsWith('data/out/files/', $fileNames['my-file']);
    }

    public function testListManifestsNonExistentDir(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'json'
        );
        self::expectException(OutputOperationException::class);
        self::expectExceptionMessage('non-existent-directory/and-file" directory does not exist.".');
        $strategy->listManifests('non-existent-directory/and-file');
    }

    public function testListManifests(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'json'
        );
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/data/out/files');
        $fs->mkdir($this->temp->getTmpFolder() . '/data/out/tables');
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-file',
            'my-contents'
        );
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-file.manifest',
            'manifest data'
        );
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-second-file',
            'second file'
        );
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-second-file.manifest',
            '2nd manifest data'
        );
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/tables/my-file',
            'my-contents'
        );
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/tables/my-file.manifest',
            'table manifest'
        );
        $files = $strategy->listManifests('data/out/files');
        $fileNames = [];
        foreach ($files as $file) {
            $fileNames[$file->getName()] = $file->getPath();
        }
        $keys = array_keys($fileNames);
        sort($keys);
        self::assertEquals(['my-file.manifest', 'my-second-file.manifest'], $keys);
        self::assertStringEndsWith('data/out/files/', $fileNames['my-file.manifest']);
    }

    public function testLoadFileToStorageEmptyConfig(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'json'
        );
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/data/out/files');
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-file_one',
            'my-data'
        );
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-file_one.manifest',
            'manifest data'
        );
        $fileId = $strategy->loadFileToStorage('/data/out/files/my-file_one', []);
        $this->clientWrapper->getBasicClient()->getFile($fileId);
        $destination = $this->temp->getTmpFolder() . 'destination';
        $this->clientWrapper->getBasicClient()->downloadFile($fileId, $destination);
        $contents = (string) file_get_contents($destination);
        self::assertEquals('my-data', $contents);

        $file = $this->clientWrapper->getBasicClient()->getFile($fileId);
        self::assertEquals($fileId, $file['id']);
        self::assertEquals('my_file_one', $file['name']);
        self::assertEquals([], $file['tags']);
        self::assertFalse($file['isPublic']);
        self::assertTrue($file['isEncrypted']);
        self::assertEquals(15, $file['maxAgeDays']);
    }

    public function testLoadFileToStorageFullConfig(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'json'
        );
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/data/out/files');
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-file_one',
            'my-data'
        );
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-file_one.manifest',
            'manifest data'
        );
        $fileId = $strategy->loadFileToStorage(
            'data/out/files/my-file_one',
            [
                'notify' => false,
                'tags' => ['first-tag', 'second-tag'],
                'is_public' => false,
                'is_permanent' => true,
                'is_encrypted' => true,
            ]
        );
        $this->clientWrapper->getBasicClient()->getFile($fileId);
        $destination = $this->temp->getTmpFolder() . 'destination';
        $this->clientWrapper->getBasicClient()->downloadFile($fileId, $destination);
        $contents = (string) file_get_contents($destination);
        self::assertEquals('my-data', $contents);

        $file = $this->clientWrapper->getBasicClient()->getFile($fileId);
        self::assertEquals($fileId, $file['id']);
        self::assertEquals('my_file_one', $file['name']);
        self::assertEquals(['first-tag', 'second-tag'], $file['tags']);
        self::assertFalse($file['isPublic']);
        self::assertTrue($file['isEncrypted']);
        self::assertNull($file['maxAgeDays']);
    }

    public function testLoadFileToStorageFileDoesNotExist(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'json'
        );
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/data/out/files');
        self::expectException(ClientException::class);
        self::expectExceptionMessage('File is not readable:');
        $strategy->loadFileToStorage('/data/out/files/non-existent', []);
    }

    public function testReadFileManifestFull(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'json'
        );
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/data/out/files');
        file_put_contents($this->temp->getTmpFolder() . '/data/out/files/my-file_one', 'my-data');
        $sourceData = [
            'is_public' => true,
            'is_permanent' => true,
            'is_encrypted' => true,
            'notify' => false,
            'tags' => [
                'my-first-tag',
                'second-tag',
            ],
        ];
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-file_one.manifest',
            json_encode($sourceData)
        );
        $manifestData = $strategy->readFileManifest('data/out/files/my-file_one.manifest');
        self::assertEquals(
            $sourceData,
            $manifestData
        );
    }

    public function testReadFileManifestFullYaml(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'yaml'
        );
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/data/out/files');
        file_put_contents($this->temp->getTmpFolder() . '/data/out/files/my-file_one', 'my-data');
        $sourceData = [
            'is_public' => true,
            'is_permanent' => true,
            'is_encrypted' => true,
            'notify' => false,
            'tags' => [
                'my-first-tag',
                'second-tag',
            ],
        ];
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-file_one.manifest',
            Yaml::dump($sourceData)
        );
        $manifestData = $strategy->readFileManifest('data/out/files/my-file_one.manifest');
        self::assertEquals(
            $sourceData,
            $manifestData
        );
    }

    public function testReadFileManifestEmpty(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'json'
        );
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/data/out/files');
        file_put_contents($this->temp->getTmpFolder() . '/data/out/files/my-file_one', 'my-data');
        $expectedData = [
            'is_public' => false,
            'is_permanent' => false,
            'is_encrypted' => true,
            'notify' => false,
            'tags' => [],
        ];
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-file_one.manifest',
            json_encode(new stdClass())
        );
        $manifestData = $strategy->readFileManifest('/data/out/files/my-file_one.manifest');
        self::assertEquals(
            $expectedData,
            $manifestData
        );
    }

    public function testReadFileManifestNotExists(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'json'
        );
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/data/out/files');
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage(
            '/data/out/files/my-file_one.manifest\' not found.'
        );
        $strategy->readFileManifest('data/out/files/my-file_one.manifest');
    }

    public function testReadFileManifestInvalid(): void
    {
        $strategy = new Local(
            $this->clientWrapper,
            new TestLogger(),
            $this->getProvider(),
            $this->getProvider(),
            'json'
        );
        $fs = new Filesystem();
        $fs->mkdir($this->temp->getTmpFolder() . '/data/out/files');
        file_put_contents(
            $this->temp->getTmpFolder() . '/data/out/files/my-file_one.manifest',
            'not a valid json'
        );
        self::expectException(InvalidOutputException::class);
        self::expectExceptionMessage(
            'data/out/files/my-file_one.manifest" as "json": Syntax error'
        );
        $strategy->readFileManifest('data/out/files/my-file_one.manifest');
    }
}
