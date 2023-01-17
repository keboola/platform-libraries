<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Configuration\File\Manifest;

use Keboola\OutputMapping\Configuration\File\Manifest\Adapter;
use PHPUnit\Framework\TestCase;

class AdapterTest extends TestCase
{
    public function testAccessors(): void
    {
        $adapter = new Adapter('json');
        self::assertEquals('json', $adapter->getFormat());
        self::assertEquals('.json', $adapter->getFileExtension());
        $adapter->setFormat('yaml');
        self::assertEquals('yaml', $adapter->getFormat());
        self::assertEquals('.yml', $adapter->getFileExtension());
        self::assertEquals(null, $adapter->getConfig());
        $adapter->setConfig(['is_permanent' => false]);
        self::assertEquals(
            [
                'is_permanent' => false,
                'tags' => [],
                'is_public' => false,
                'is_encrypted' => true,
                'notify' => false,
            ],
            $adapter->getConfig()
        );
    }

    public function testDeserializeJson(): void
    {
        $adapter = new Adapter('json');
        $data = $adapter->deserialize('{"is_permanent": false}');
        self::assertEquals(
            [
                'is_permanent' => false,
                'tags' => [],
                'is_public' => false,
                'is_encrypted' => true,
                'notify' => false,
            ],
            $data
        );
    }

    public function testDeserializeYaml(): void
    {
        $adapter = new Adapter('yaml');
        $data = $adapter->deserialize('is_permanent: true');
        self::assertEquals(
            [
                'is_permanent' => true,
                'tags' => [],
                'is_public' => false,
                'is_encrypted' => true,
                'notify' => false,
            ],
            $data
        );
    }
}
