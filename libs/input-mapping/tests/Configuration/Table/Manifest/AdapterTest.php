<?php

namespace Keboola\InputMapping\Tests\Configuration\Table\Manifest;

use Generator;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
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
    "id": "in.c-bucket.test",
    "primary_key": [],
    "distribution_key": [],
    "columns": [],
    "metadata": [],
    "column_metadata": []
}
EOF,
        ];
        yield 'json format' => [
            'format' => 'json',
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
            'format' => 'yaml',
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
        ?string $format,
        string $expectedData
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
        yield 'default format' => [
            'format' => null,
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
        yield 'json format' => [
            'format' => 'json',
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
            'format' => 'yaml',
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
        ?string $format,
        string $expectedFilePathname
    ): void {
        $temp = new Temp('docker');
        $temp->initRunFolder();

        $filePathname = (string) $temp->createTmpFile();

        if ($format) {
            $adapter = new Adapter($format);
        } else {
            $adapter = new Adapter();
        }

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
