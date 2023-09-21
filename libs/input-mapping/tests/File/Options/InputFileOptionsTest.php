<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\File\Options;

use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\File\Options\InputFileOptions;
use PHPUnit\Framework\TestCase;

class InputFileOptionsTest extends TestCase
{
    public function testGetters(): void
    {
        $options = new InputFileOptions(['tags' => ['foo']], false, '1234');
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
    }

    public function testQueryBranch(): void
    {
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(
            'Invalid file mapping, the \'query\' attribute is unsupported in the dev/branch context.',
        );
        new InputFileOptions(['query' => 'will not work'], true, '1234');
    }
}
