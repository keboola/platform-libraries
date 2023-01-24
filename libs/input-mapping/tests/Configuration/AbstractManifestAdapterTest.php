<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Configuration;

use Generator;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InputOperationException;
use PHPUnit\Framework\TestCase;

abstract class AbstractManifestAdapterTest extends TestCase
{
    public function initWithFormatData(): Generator
    {
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
        // @phpstan-ignore-next-line Deliberately supplying invalid value
        new Adapter('test');
    }
}
