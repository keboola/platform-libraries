<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Needs;

use Attribute;

#[Attribute] class NeedsEmptyRedshiftInputBucket
{
    public function __construct()
    {
    }
}
