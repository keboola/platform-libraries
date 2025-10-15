<?php

declare(strict_types=1);

namespace Keboola\PhpTestUtils\Fixtures;

use Attribute;

#[Attribute]
class FixtureBackend
{
    public function __construct(
        public BackendType $backend,
    ) {
    }
}
