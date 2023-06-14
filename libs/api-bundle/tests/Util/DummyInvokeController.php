<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Util;

class DummyInvokeController
{
    public function __invoke(): void
    {
    }
}
