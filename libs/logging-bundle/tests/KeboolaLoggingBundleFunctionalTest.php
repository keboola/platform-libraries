<?php

declare(strict_types=1);

namespace Keboola\LoggingBundle\Tests;

use Keboola\LoggingBundle\Monolog\DataDogContextProcessor;
use Monolog\Logger;
use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;

class KeboolaLoggingBundleFunctionalTest extends KernelTestCase
{
    public function testMonologProcessorIsRegistered(): void
    {
        $container = self::getContainer();

        $logger = $container->get('logger');
        self::assertInstanceOf(Logger::class, $logger);

        $datadogProcessors = array_filter(
            $logger->getProcessors(),
            fn (callable $processor) => $processor instanceof DataDogContextProcessor
        );
        self::assertCount(1, $datadogProcessors);
    }
}
