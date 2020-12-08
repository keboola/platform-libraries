<?php

namespace Keboola\OutputMapping\Tests\Configuration\File\Manifest;

use Keboola\OutputMapping\Configuration\File\Manifest\Adapter;
use Keboola\OutputMapping\Exception\OutputOperationException;
use PHPUnit\Framework\TestCase;

class AdapterTest extends TestCase
{
    public function testAccessors()
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

    public function testInvalidFormat()
    {
        self::expectException(OutputOperationException::class);
        self::expectExceptionMessage('Configuration format \'invalid\' not supported');
        new Adapter('invalid');
    }

    public function testInvalidSetFormat()
    {
        $adapter = new Adapter('json');
        self::expectException(OutputOperationException::class);
        self::expectExceptionMessage('Configuration format \'invalid\' not supported');
        $adapter->setFormat('invalid');
    }

    public function testDeserializeJson()
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

    public function testDeserializeYaml()
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
