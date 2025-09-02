<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use InvalidArgumentException;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\AbstractWorkspaceStrategy;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use ReflectionClass;

class AbstractWorkspaceStrategyTest extends TestCase
{
    public function testConstructorValidatesDataStorageIsWorkspaceStagingInterface(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Data storage must be instance of WorkspaceStagingInterface');

        new class(
            $this->createMock(ClientWrapper::class),
            $this->createMock(LoggerInterface::class),
            $this->createMock(StagingInterface::class), // This should fail validation
            $this->createMock(FileStagingInterface::class),
            $this->createMock(InputTableStateList::class),
            'destination',
            FileFormat::Json,
        ) extends AbstractWorkspaceStrategy {
            public function getWorkspaceType(): string
            {
                return 'test';
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
            $this->createMock(WorkspaceStagingInterface::class),
            $metadataStorage,
            $this->createMock(InputTableStateList::class),
            $destination,
            FileFormat::Json,
        ) extends AbstractWorkspaceStrategy {
            public function getWorkspaceType(): string
            {
                return 'test';
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
