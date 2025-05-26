<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Configuration\Table\Manifest;

use Generator;
use Keboola\InputMapping\Configuration\Adapter as BaseAdapter;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
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

    public function setConfigAndSerializeData(): Generator
    {
        yield 'json format' => [
            'format' => FileFormat::Json,
            'expectedData' => <<<'EOF'
{
    "id": "in.c-bucket.test",
    "primary_key": [],
    "distribution_key": [],
    "columns": [],
    "metadata": [],
    "column_metadata": []
}
EOF,
        ];
        yield 'yaml format' => [
            'format' => FileFormat::Yaml,
            'expectedData' => <<<'EOF'
id: in.c-bucket.test
primary_key: {  }
distribution_key: {  }
columns: {  }
metadata: {  }
column_metadata: {  }

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
        $adapter->setConfig(['id' => 'in.c-bucket.test']);

        self::assertSame($expectedData, $adapter->serialize());
    }

    public function testSetInvalidConfigThrowsException(): void
    {
        $adapter = new Adapter();

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The child config "id" under "table" must be configured.');

        $adapter->setConfig([]);
    }

    public function fileOperationsData(): Generator
    {
        yield 'json format' => [
            'format' => FileFormat::Json,
            'expectedData' => <<<'EOF'
{
    "id": "in.c-bucket.test",
    "primary_key": [],
    "distribution_key": [],
    "columns": [],
    "metadata": [],
    "column_metadata": []
}
EOF,
        ];
        yield 'yaml format' => [
            'format' => FileFormat::Yaml,
            'expectedData' => <<<'EOF'
id: in.c-bucket.test
primary_key: {  }
distribution_key: {  }
columns: {  }
metadata: {  }
column_metadata: {  }

EOF,
        ];
    }

    /**
     * @dataProvider fileOperationsData
     */
    public function testFileOperations(
        FileFormat $format,
        string $expectedFilePathname,
    ): void {
        $temp = new Temp('docker');
        $filePathname = (string) $temp->createTmpFile();
        $adapter = new Adapter($format);

        $adapter->setConfig(['id' => 'in.c-bucket.test']);
        $adapter->writeToFile($filePathname);

        self::assertSame($expectedFilePathname, file_get_contents($filePathname));
        self::assertSame($expectedFilePathname, $adapter->getContents($filePathname));

        self::assertSame([
            'id' => 'in.c-bucket.test',
            'primary_key' => [],
            'distribution_key' => [],
            'columns' => [],
            'metadata' => [],
            'column_metadata' => [],
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
