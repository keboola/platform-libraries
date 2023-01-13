<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Options;

use Keboola\InputMapping\Table\Options\ReaderOptions;
use PHPUnit\Framework\TestCase;

class ReaderOptionsTest extends TestCase
{
    public function testAccessors(): void
    {
        $options = new ReaderOptions(true);
        self::assertTrue($options->devInputsDisabled());
        $options = new ReaderOptions(false);
        self::assertFalse($options->devInputsDisabled());
    }
}
