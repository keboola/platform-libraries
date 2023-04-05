<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Needs;

use Attribute;

#[Attribute] class NeedsTestTables
{
    public function __construct(public readonly int $count = 1)
    {
    }
}
