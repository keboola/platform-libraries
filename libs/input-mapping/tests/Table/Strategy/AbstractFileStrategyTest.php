<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use InvalidArgumentException;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\AbstractFileStrategy;
use Keboola\InputMapping\Table\Strategy\TableExportQueue;
use Keboola\InputMapping\Table\Strategy\TableLoadQueueInterface;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

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
            public function prepareAndExecuteTableLoads(array $tables, bool $preserve): TableLoadQueueInterface
            {
                return new TableExportQueue([]);
            }

            protected function materializeTableLoads(TableLoadQueueInterface $queue, array $jobResults): void
            {
            }
        };
    }
}
