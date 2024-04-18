<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Needs;

use Attribute;

#[Attribute] class NeedsRemoveBucket
{
    public function __construct(public readonly string $bucketName = 'in.c-main')
    {
    }
}
