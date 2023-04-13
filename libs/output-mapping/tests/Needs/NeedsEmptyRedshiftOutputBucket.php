<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Needs;

use Attribute;

#[Attribute] class NeedsEmptyRedshiftOutputBucket
{
    public function __construct()
    {
    }
}
