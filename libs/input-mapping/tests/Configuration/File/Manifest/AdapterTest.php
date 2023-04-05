<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Configuration\File\Manifest;

use Generator;
use Keboola\InputMapping\Configuration\Adapter as BaseAdapter;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Tests\Configuration\AbstractManifestAdapterTest;
use Keboola\Temp\Temp;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class AdapterTest extends AbstractManifestAdapterTest
{
    /**
     * @param BaseAdapter::FORMAT_YAML | BaseAdapter::FORMAT_JSON $format
     */
    private function createAdapter(string $format): Adapter
    {
        return new Adapter($format);
    }

    /**
     * @param BaseAdapter::FORMAT_YAML | BaseAdapter::FORMAT_JSON $format
     * @dataProvider initWithFormatData
     */
    public function testInitWithFormat(
        string $format,
        string $expectedFormat,
        string $expectedExtension
    ): void {
        $adapter = $this->createAdapter($format);

        self::assertSame($expectedFormat, $adapter->getFormat());
        self::assertSame($expectedExtension, $adapter->getFileExtension());
    }

    public function setConfigAndSerializeData(): Generator
    {
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
     * @param BaseAdapter::FORMAT_YAML | BaseAdapter::FORMAT_JSON $format
     * @dataProvider setConfigAndSerializeData
     */
    public function testSetConfigAndSerialize(
        string $format,
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
     * @param BaseAdapter::FORMAT_YAML | BaseAdapter::FORMAT_JSON $format
     * @dataProvider fileOperationsData
     */
    public function testFileOperations(
        string $format,
        string $expectedData
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
