<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use InvalidArgumentException;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\AbstractFileStrategy;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use ReflectionClass;

class AbstractFileStrategyTest extends TestCase
{
    public function testConstructorValidatesDataStorageIsFileStagingInterface(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Data storage must be instance of FileStagingInterface');

        new class(
            $this->createMock(ClientWrapper::class),
            $this->createMock(LoggerInterface::class),
            $this->createMock(StagingInterface::class), // This should fail validation
            $this->createMock(FileStagingInterface::class),
            $this->createMock(InputTableStateList::class),
            'destination',
            FileFormat::Json,
        ) extends AbstractFileStrategy {
            public function downloadTable(RewrittenInputTableOptions $table): array
            {
                return [];
            }

            public function handleExports(array $exports, bool $preserve): array
            {
                return [];
            }
        };
    }

    public function testGetters(): void
    {
        $metadataStorage = $this->createMock(FileStagingInterface::class);
        $destination = 'test-destination';

        $strategy = new class(
            $this->createMock(ClientWrapper::class),
            $this->createMock(LoggerInterface::class),
            $this->createMock(FileStagingInterface::class),
            $metadataStorage,
            $this->createMock(InputTableStateList::class),
            $destination,
            FileFormat::Json,
        ) extends AbstractFileStrategy {
            public function downloadTable(RewrittenInputTableOptions $table): array
            {
                return [];
            }

            public function handleExports(array $exports, bool $preserve): array
            {
                return [];
            }
        };

        $reflection = new ReflectionClass($strategy);

        $getMetadataStorageMethod = $reflection->getMethod('getMetadataStorage');
        $getMetadataStorageMethod->setAccessible(true);
        $this->assertSame($metadataStorage, $getMetadataStorageMethod->invoke($strategy));

        $getDestinationMethod = $reflection->getMethod('getDestination');
        $getDestinationMethod->setAccessible(true);
        $this->assertSame($destination, $getDestinationMethod->invoke($strategy));
    }
}
