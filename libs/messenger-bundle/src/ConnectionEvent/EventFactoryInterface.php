<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\ConnectionEvent;

use Keboola\MessengerBundle\ConnectionEvent\Exception\EventFactoryException;

interface EventFactoryInterface
{
    /**
     * @throws EventFactoryException
     */
    public function createEventFromArray(array $data): EventInterface;
}
