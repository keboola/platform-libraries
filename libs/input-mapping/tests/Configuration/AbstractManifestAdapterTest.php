<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Configuration;

use Keboola\StagingProvider\Staging\File\FileFormat;
use PHPUnit\Framework\TestCase;

abstract class AbstractManifestAdapterTest extends TestCase
{
    public function initWithFormatData(): iterable
    {
        yield 'json format' => [
            'format' => FileFormat::Json,
            'expectedFormat' => FileFormat::Json,
            'expectedExtension' => '.json',
        ];
        yield 'yaml format' => [
            'format' => FileFormat::Yaml,
            'expectedFormat' => FileFormat::Yaml,
            'expectedExtension' => '.yml',
        ];
    }
}
