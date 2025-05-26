<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Configuration\File\Manifest;

use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Tests\Configuration\AbstractManifestAdapterTest;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\Temp\Temp;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class AdapterTest extends AbstractManifestAdapterTest
{
    private function createAdapter(FileFormat $format): Adapter
    {
        return new Adapter($format);
    }

    /**
     * @dataProvider initWithFormatData
     */
    public function testInitWithFormat(
        FileFormat $format,
        FileFormat $expectedFormat,
        string $expectedExtension,
    ): void {
        $adapter = $this->createAdapter($format);

        self::assertSame($expectedFormat, $adapter->getFormat());
        self::assertSame($expectedExtension, $adapter->getFileExtension());
    }

    public function setConfigAndSerializeData(): iterable
    {
        yield 'json format' => [
            'format' => FileFormat::Json,
            'expectedData' => <<<'EOF'
{
    "id": 12345678,
    "is_public": false,
    "is_encrypted": false,
    "is_sliced": false,
    "tags": []
}
EOF,
        ];
        yield 'yaml format' => [
            'format' => FileFormat::Yaml,
            'expectedData' => <<<'EOF'
id: 12345678
is_public: false
is_encrypted: false
is_sliced: false
tags: {  }

EOF,
        ];
    }

    /**
     * @dataProvider setConfigAndSerializeData
     */
    public function testSetConfigAndSerialize(
        FileFormat $format,
        string $expectedData,
    ): void {
        $adapter = $this->createAdapter($format);
        $adapter->setConfig(['id' => 12345678]);

        self::assertSame($expectedData, $adapter->serialize());
    }

    public function testSetInvalidConfigThrowsException(): void
    {
        $adapter = new Adapter();

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The child config "id" under "file" must be configured.');

        $adapter->setConfig([]);
    }

    public function fileOperationsData(): iterable
    {
        yield 'json format' => [
            'format' => FileFormat::Json,
            'expectedData' => <<<'EOF'
{
    "id": 12345678,
    "is_public": false,
    "is_encrypted": false,
    "is_sliced": false,
    "tags": []
}
EOF,
        ];
        yield 'yaml format' => [
            'format' => FileFormat::Yaml,
            'expectedData' => <<<'EOF'
id: 12345678
is_public: false
is_encrypted: false
is_sliced: false
tags: {  }

EOF,
        ];
    }

    /**
     * @dataProvider fileOperationsData
     */
    public function testFileOperations(
        FileFormat $format,
        string $expectedData,
    ): void {
        $temp = new Temp('docker');

        $filePathname = (string) $temp->createTmpFile();
        $adapter = new Adapter($format);

        $adapter->setConfig(['id' => 12345678]);
        $adapter->writeToFile($filePathname);

        self::assertSame($expectedData, file_get_contents($filePathname));
        self::assertSame($expectedData, $adapter->getContents($filePathname));

        self::assertSame([
            'id' => 12345678,
            'is_public' => false,
            'is_encrypted' => false,
            'is_sliced' => false,
            'tags' => [],
        ], $adapter->readFromFile($filePathname));
    }

    public function testGetContentsOfNonExistingFileThrowsException(): void
    {
        $adapter = new Adapter();

        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage('File \'non-existent\' not found.');

        $adapter->readFromFile('non-existent');
    }

    public function testReadFromNonExistingFileThrowsException(): void
    {
        $adapter = new Adapter();

        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage('File \'non-existent\' not found.');

        $adapter->readFromFile('non-existent');
    }
}
