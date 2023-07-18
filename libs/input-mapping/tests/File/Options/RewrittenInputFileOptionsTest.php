<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\File\Options;

use Keboola\InputMapping\File\Options\RewrittenInputFileOptions;
use PHPUnit\Framework\TestCase;

class RewrittenInputFileOptionsTest extends TestCase
{
    public function testGetters(): void
    {
        $options = new RewrittenInputFileOptions(
            ['tags' => ['foo']],
            false,
            '1234',
            ['tags' => ['bar']],
        );

        self::assertSame(['foo'], $options->getTags());
        self::assertFalse($options->isDevBranch());
        self::assertSame('1234', $options->getRunId());
        self::assertSame(
            [
                'tags' => ['foo'],
                'overwrite' => true,
            ],
            $options->getDefinition(),
        );
        self::assertSame(
            [['name' => 'bar']],
            $options->getFileConfigurationIdentifier()
        );
    }
}
