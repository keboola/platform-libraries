<?php

declare(strict_types=1);

namespace Keboola\Settle\Comparator;

/**
 * @extends IsSame<bool>
 */
class IsTrue extends IsSame
{
    public function __construct()
    {
        parent::__construct(true);
    }
}
