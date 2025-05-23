<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration\File\Manifest;

use Keboola\OutputMapping\Configuration\File\Manifest\Adapter;
use Keboola\StagingProvider\Staging\File\FileFormat;
use PHPUnit\Framework\TestCase;

class AdapterTest extends TestCase
{
    public function provideReadFileManifestInvalid(): iterable
    {
        yield 'json' => [
            'format' => FileFormat::Json,
            'expectedFileExtension' => '.json',
        ];
        yield 'format' => [
            'format' => FileFormat::Yaml,
            'expectedFileExtension' => '.yml',
        ];
    }

    /** @dataProvider provideReadFileManifestInvalid */
    public function testAccessors(
        FileFormat $format,
        string $expectedFileExtension,
    ): void {
        $adapter = new Adapter($format);
        self::assertSame($format, $adapter->getFormat());
        self::assertSame($expectedFileExtension, $adapter->getFileExtension());
        self::assertSame(null, $adapter->getConfig());

        $adapter->setConfig(['is_permanent' => false]);
        self::assertSame(
            [
                'is_permanent' => false,
                'tags' => [],
                'is_public' => false,
                'is_encrypted' => true,
                'notify' => false,
            ],
            $adapter->getConfig(),
        );
    }

    public function testDeserializeJson(): void
    {
        $adapter = new Adapter(FileFormat::Json);
        $data = $adapter->deserialize('{"is_permanent": false}');
        self::assertEquals(
            [
                'is_permanent' => false,
                'tags' => [],
                'is_public' => false,
                'is_encrypted' => true,
                'notify' => false,
            ],
            $data,
        );
    }

    public function testDeserializeYaml(): void
    {
        $adapter = new Adapter(FileFormat::Yaml);
        $data = $adapter->deserialize('is_permanent: true');
        self::assertEquals(
            [
                'is_permanent' => true,
                'tags' => [],
                'is_public' => false,
                'is_encrypted' => true,
                'notify' => false,
            ],
            $data,
        );
    }
}
