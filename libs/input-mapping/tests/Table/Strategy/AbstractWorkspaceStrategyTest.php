<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use InvalidArgumentException;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\AbstractWorkspaceStrategy;
use Keboola\InputMapping\Table\Strategy\WorkspaceLoadType;
use Keboola\InputMapping\Table\Strategy\WorkspaceTableLoadInstruction;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\StorageApiToken;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use ReflectionClass;

class AbstractWorkspaceStrategyTest extends TestCase
{
    private TestHandler $testHandler;
    private Logger $testLogger;

    public function setUp(): void
    {
        parent::setUp();

        $this->testHandler = new TestHandler();
        $this->testLogger = new Logger('testLogger', [$this->testHandler]);
    }


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
        self::assertSame($metadataStorage, $getMetadataStorageMethod->invoke($strategy));

        $getDestinationMethod = $reflection->getMethod('getDestination');
        $getDestinationMethod->setAccessible(true);
        self::assertSame($destination, $getDestinationMethod->invoke($strategy));
    }

    public function testPrepareTableLoadsToWorkspaceClone(): void
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getToken')
            ->willReturn(new StorageApiToken(
                [
                    'owner' => ['id' => 12345],
                ],
                'my-secret-token',
            ))
        ;

        $strategy = $this->createTestStrategy($clientWrapper, 'snowflake');

        // Table that can be cloned (Snowflake backend, no filters)
        $tableOptions = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table1', 'destination' => 'table1'],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'snowflake'],
                'isAlias' => false,
            ],
        );

        $instructions = $strategy->prepareTableLoadsToWorkspace([$tableOptions]);

        self::assertCount(1, $instructions);
        self::assertInstanceOf(WorkspaceTableLoadInstruction::class, $instructions[0]);
        self::assertSame(WorkspaceLoadType::CLONE, $instructions[0]->loadType);
        self::assertSame($tableOptions, $instructions[0]->table);
        self::assertNull($instructions[0]->loadOptions);

        self::assertTrue($this->testHandler->hasInfoThatContains('Table "in.c-test-bucket.table1" will be cloned.'));
    }

    public function testPrepareTableLoadsToWorkspaceView(): void
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getToken')
            ->willReturn(new StorageApiToken(
                [
                    'owner' => ['id' => 12345],
                ],
                'my-secret-token',
            ))
        ;

        $strategy = $this->createTestStrategy($clientWrapper, 'bigquery');

        // Table that can use view (BigQuery backend)
        $tableOptions = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table1', 'destination' => 'table1'],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'bigquery'],
                'isAlias' => false,
            ],
        );

        $instructions = $strategy->prepareTableLoadsToWorkspace([$tableOptions]);

        self::assertCount(1, $instructions);
        self::assertEquals(WorkspaceLoadType::VIEW, $instructions[0]->loadType);
        self::assertSame($tableOptions, $instructions[0]->table);
        self::assertSame(['overwrite' => false], $instructions[0]->loadOptions);

        self::assertTrue(
            $this->testHandler->hasInfoThatContains('Table "in.c-test-bucket.table1" will be created as view.'),
        );
    }

    public function testPrepareTableLoadsToWorkspaceCopy(): void
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getToken')
            ->willReturn(new StorageApiToken(
                [
                    'owner' => ['id' => 12345],
                ],
                'my-secret-token',
            ))
        ;

        $strategy = $this->createTestStrategy($clientWrapper, 'snowflake');

        // Table that must be copied (Different backend than workspace)
        $tableOptions = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table1', 'destination' => 'table1'],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'bigquery'],
                'isAlias' => false,
            ],
        );

        $instructions = $strategy->prepareTableLoadsToWorkspace([$tableOptions]);

        self::assertCount(1, $instructions);
        self::assertEquals(WorkspaceLoadType::COPY, $instructions[0]->loadType);
        self::assertSame($tableOptions, $instructions[0]->table);
        self::assertSame(['overwrite' => false], $instructions[0]->loadOptions);

        self::assertTrue($this->testHandler->hasInfoThatContains('Table "in.c-test-bucket.table1" will be copied.'));
    }

    public function testPrepareTableLoadsToWorkspaceChecksViableLoadMethod(): void
    {
        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper->expects($this->once())
            ->method('getToken')
            ->willReturn(new StorageApiToken(
                [
                    'owner' => ['id' => 12345],
                ],
                'my-secret-token',
            ))
        ;

        $strategy = $this->createTestStrategy($clientWrapper, 'bigquery');

        // Create a table that will cause checkViableLoadMethod to throw an exception
        // (BigQuery workspace with alias table from the same project)
        $tableOptions = new RewrittenInputTableOptions(
            ['source' => 'in.c-test-bucket.table1', 'destination' => 'table1'],
            'in.c-test-bucket.table1',
            123,
            [
                'id' => 'in.c-test-bucket.table1',
                'bucket' => ['backend' => 'bigquery'],
                'isAlias' => true,
                'sourceTable' => ['project' => ['id' => 12345]], // Same project ID
            ],
        );

        // This should throw an InvalidInputException because checkViableLoadMethod is called
        $this->expectException(InvalidInputException::class);
        $this->expectExceptionMessage(
            'Table "in.c-test-bucket.table1" is an alias, which is not supported when loading Bigquery tables.',
        );

        $strategy->prepareTableLoadsToWorkspace([$tableOptions]);
    }

    private function createTestStrategy(
        ClientWrapper $clientWrapper,
        string $workspaceType,
    ): TestWorkspaceStrategy {
        $stragegy = new TestWorkspaceStrategy(
            $clientWrapper,
            $this->testLogger,
            $this->createMock(WorkspaceStagingInterface::class),
            $this->createMock(FileStagingInterface::class),
            $this->createMock(InputTableStateList::class),
            'destination',
            FileFormat::Json,
        );
        $stragegy->setWorkspaceType($workspaceType);
        return $stragegy;
    }
}
