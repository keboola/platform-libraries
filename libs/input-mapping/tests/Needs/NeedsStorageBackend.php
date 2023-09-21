<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Needs;

use Attribute;

#[Attribute] class NeedsStorageBackend
{
    public function __construct(public readonly string $backend)
    {
    }
}
