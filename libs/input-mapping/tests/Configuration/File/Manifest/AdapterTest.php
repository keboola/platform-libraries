<?php

namespace Keboola\InputMapping\Tests\Configuration\File\Manifest;

use Generator;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Tests\Configuration\AbstractManifestAdapterTest;
use Keboola\Temp\Temp;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class AdapterTest extends AbstractManifestAdapterTest
{
    private function createAdapter(?string $format): Adapter
    {
        if ($format) {
            return new Adapter($format);
        }

        return new Adapter();
    }

    /**
     * @dataProvider initWithFormatData
     */
    public function testInitWithFormat(
        ?string $format,
        string $expectedFormat,
        string $expectedExtension
    ): void {
        $adapter = $this->createAdapter($format);

        self::assertSame($expectedFormat, $adapter->getFormat());
        self::assertSame($expectedExtension, $adapter->getFileExtension());
    }

    public function setConfigAndSerializeData(): Generator
    {
        yield 'default format' => [
            'format' => null,
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
        yield 'json format' => [
            'format' => 'json',
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
            'format' => 'yaml',
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
        ?string $format,
        string $expectedData
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

    public function fileOperationsData(): Generator
    {
        yield 'default format' => [
            'format' => null,
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
        yield 'json format' => [
            'format' => 'json',
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
            'format' => 'yaml',
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
        ?string $format,
        string $expectedData
    ): void {
        $temp = new Temp('docker');
        $temp->initRunFolder();

        $filePathname = (string) $temp->createTmpFile();

        if ($format) {
            $adapter = new Adapter($format);
        } else {
            $adapter = new Adapter();
        }

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
        $this->expectExceptionMessage('File \'test\' not found.');

        $adapter->readFromFile('test');
    }

    public function testReadFromNonExistingFileThrowsException(): void
    {
        $adapter = new Adapter();

        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage('File \'test\' not found.');

        $adapter->readFromFile('test');
    }
}
