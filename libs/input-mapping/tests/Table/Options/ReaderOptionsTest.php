<?php

namespace Keboola\InputMapping\Tests\Table\Options;

use Keboola\InputMapping\Table\Options\ReaderOptions;
use PHPUnit\Framework\TestCase;

class ReaderOptionsTest extends TestCase
{
    public function testAccessors()
    {
        $options = new ReaderOptions(true);
        self::assertTrue($options->devInputsDisabled());
        $options = new ReaderOptions(false);
        self::assertFalse($options->devInputsDisabled());
    }
}
