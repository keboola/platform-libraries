<?php

declare(strict_types=1);

namespace Keboola\ApiBundle\Tests\Security\StorageApiToken;

use Retry\BackOff\SleeperInterface;

class RecordingSleeper implements SleeperInterface
{
    /** @var list<int> */
    public array $periods = [];

    public function sleep(int $period): void
    {
        $this->periods[] = $period;
    }
}
