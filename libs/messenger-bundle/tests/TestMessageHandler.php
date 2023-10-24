<?php

declare(strict_types=1);

namespace Keboola\MessengerBundle\Tests;

use Keboola\MessengerBundle\ConnectionEvent\EventInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Messenger\Attribute\AsMessageHandler;

#[AsMessageHandler]
class TestMessageHandler
{
    public function __construct(
        private readonly LoggerInterface $logger,
    ) {
    }

    public function __invoke(EventInterface $event): void
    {
        $this->logger->debug(sprintf(
            'Event %s received: %s',
            $event::class,
            json_encode($event->toArray(), JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE),
        ));
    }
}
