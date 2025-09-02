<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Generator;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\AbstractStrategy;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use ReflectionClass;

class AbstractStrategyTest extends TestCase
{
    public static function getManifestPathDataProvider(): Generator
    {
        yield 'basic case with custom destination' => [
            'metadataPath' => '/tmp/metadata',
            'destination' => 'destination-folder',
            'tableSource' => 'test-table',
            'tableDestination' => 'my-table',
            'expected' => '/tmp/metadata/destination-folder/my-table.manifest',
        ];

        yield 'table without custom destination (uses source)' => [
            'metadataPath' => '/tmp/metadata',
            'destination' => 'destination-folder',
            'tableSource' => 'test-table',
            'tableDestination' => null,
            'expected' => '/tmp/metadata/destination-folder/test-table.manifest',
        ];

        yield 'metadata path with trailing slash' => [
            'metadataPath' => '/tmp/metadata/',
            'destination' => 'destination-folder',
            'tableSource' => 'test-table',
            'tableDestination' => 'my-table',
            'expected' => '/tmp/metadata/destination-folder/my-table.manifest',
        ];

        yield 'metadata path without leading slash' => [
            'metadataPath' => 'tmp/metadata',
            'destination' => 'destination-folder',
            'tableSource' => 'test-table',
            'tableDestination' => 'my-table',
            'expected' => 'tmp/metadata/destination-folder/my-table.manifest',
        ];

        yield 'complex paths with multiple segments' => [
            'metadataPath' => '/var/tmp/keboola/metadata',
            'destination' => 'input/tables',
            'tableSource' => 'bucket.table-name',
            'tableDestination' => 'transformed-table',
            'expected' => '/var/tmp/keboola/metadata/input/tables/transformed-table.manifest',
        ];

        yield 'metadata path with multiple trailing slashes' => [
            'metadataPath' => '/tmp/metadata///',
            'destination' => 'destination-folder',
            'tableSource' => 'test-table',
            'tableDestination' => 'my-table',
            'expected' => '/tmp/metadata/destination-folder/my-table.manifest',
        ];
    }

    /**
     * @dataProvider getManifestPathDataProvider
     */
    public function testGetManifestPath(
        string $metadataPath,
        string $destination,
        string $tableSource,
        ?string $tableDestination,
        string $expected,
    ): void {
        $metadataStorageMock = $this->createMock(FileStagingInterface::class);
        $metadataStorageMock->expects($this->once())->method('getPath')->willReturn($metadataPath);

        $strategy = new class(
            $this->createMock(ClientWrapper::class),
            $this->createMock(LoggerInterface::class),
            $this->createMock(StagingInterface::class),
            $metadataStorageMock,
            $this->createMock(InputTableStateList::class),
            $destination,
            FileFormat::Json,
        ) extends AbstractStrategy {
            public function __construct(
                ClientWrapper $clientWrapper,
                LoggerInterface $logger,
                StagingInterface $dataStorage,
                private readonly FileStagingInterface $metadataStorage,
                InputTableStateList $tablesState,
                private readonly string $destination,
                FileFormat $format,
            ) {
            }

            protected function getMetadataStorage(): FileStagingInterface
            {
                return $this->metadataStorage;
            }

            protected function getDestination(): string
            {
                return $this->destination;
            }

            public function downloadTable(RewrittenInputTableOptions $table): array
            {
                return [];
            }

            public function handleExports(array $exports, bool $preserve): array
            {
                return [];
            }
        };

        $tableOptionsConfig = ['source' => $tableSource];
        if ($tableDestination !== null) {
            $tableOptionsConfig['destination'] = $tableDestination;
        }

        $tableOptions = new RewrittenInputTableOptions(
            $tableOptionsConfig,
            $tableSource,
            123,
            [],
        );

        $reflection = new ReflectionClass($strategy);
        $method = $reflection->getMethod('getManifestPath');
        $method->setAccessible(true);

        $result = $method->invoke($strategy, $tableOptions);

        self::assertSame($expected, $result);
    }
}
