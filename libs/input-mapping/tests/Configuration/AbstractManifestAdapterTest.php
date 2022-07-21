<?php

namespace Keboola\InputMapping\Tests\Configuration;

use Generator;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InputOperationException;
use PHPUnit_Framework_TestCase;

abstract class AbstractManifestAdapterTest extends PHPUnit_Framework_TestCase
{
    public function initWithFormatData(): Generator
    {
        yield 'default format' => [
            'format' => null,
            'expectedFormat' => 'json',
            'expectedExtension' => '.json',
        ];
        yield 'json format' => [
            'format' => 'json',
            'expectedFormat' => 'json',
            'expectedExtension' => '.json',
        ];
        yield 'yaml format' => [
            'format' => 'yaml',
            'expectedFormat' => 'yaml',
            'expectedExtension' => '.yml',
        ];
    }

    public function testInitWithUnsupportedFormatThrowsException(): void
    {
        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage('Configuration format \'test\' not supported');

        new Adapter('test');
    }
}
