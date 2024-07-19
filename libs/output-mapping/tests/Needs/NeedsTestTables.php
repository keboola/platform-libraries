<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Needs;

use Attribute;

#[Attribute] class NeedsTestTables
{
    public function __construct(
        public readonly int $count = 1,
        public readonly bool $typedTable = false,
    ) {
    }
}
