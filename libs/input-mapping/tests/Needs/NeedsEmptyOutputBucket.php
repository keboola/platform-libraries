<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Needs;

use Attribute;

#[Attribute] class NeedsEmptyOutputBucket
{
    public function __construct()
    {
    }
}
