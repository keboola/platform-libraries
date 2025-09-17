<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Tests;

use Keboola\PhpTestUtils\TestEnvVarsTrait;
use PHPUnit\Framework\TestCase;

abstract class TestEnvVarsTraitAbstract extends TestCase
{
    use TestEnvVarsTrait;
}
