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
        self::assertTrue($options->preserveWorkspace());
        self::assertFalse($options->hasProtectedDefaultBranch());

        $options = new ReaderOptions(false, false, true);
        self::assertFalse($options->devInputsDisabled());
        self::assertFalse($options->preserveWorkspace());
        self::assertTrue($options->hasProtectedDefaultBranch());
    }
}
