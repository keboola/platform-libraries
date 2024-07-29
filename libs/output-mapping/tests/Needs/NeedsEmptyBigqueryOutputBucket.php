<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Needs;

use Attribute;

#[Attribute] class NeedsEmptyBigqueryOutputBucket
{
    public function __construct()
    {
    }
}
