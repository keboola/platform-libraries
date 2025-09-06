<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Table\Strategy;

use Generator;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\InputMapping\Table\Strategy\Local;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use ReflectionClass;

class LocalTest extends TestCase
{
    public static function getDataFilePathDataProvider(): Generator
    {
        yield 'basic case with custom destination' => [
            'dataPath' => '/tmp/data',
            'destination' => 'destination-folder',
            'tableSource' => 'test-table',
            'tableDestination' => 'my-table',
            'expected' => '/tmp/data/destination-folder/my-table',
        ];

        yield 'table without custom destination (uses source)' => [
            'dataPath' => '/tmp/data',
            'destination' => 'destination-folder',
            'tableSource' => 'test-table',
            'tableDestination' => null,
            'expected' => '/tmp/data/destination-folder/test-table',
        ];

        yield 'data path with trailing slash' => [
            'dataPath' => '/tmp/data/',
            'destination' => 'destination-folder',
            'tableSource' => 'test-table',
            'tableDestination' => 'my-table',
            'expected' => '/tmp/data/destination-folder/my-table',
        ];

        yield 'complex paths with multiple segments' => [
            'dataPath' => '/var/tmp/keboola/data',
            'destination' => 'input/tables',
            'tableSource' => 'bucket.table-name',
            'tableDestination' => 'transformed-table',
            'expected' => '/var/tmp/keboola/data/input/tables/transformed-table',
        ];
    }

    /**
     * @dataProvider getDataFilePathDataProvider
     */
    public function testGetDataFilePath(
        string $dataPath,
        string $destination,
        string $tableSource,
        ?string $tableDestination,
        string $expected,
    ): void {
        $dataStorage = $this->createMock(FileStagingInterface::class);
        $dataStorage->expects($this->once())
            ->method('getPath')
            ->willReturn($dataPath);

        $local = new Local(
            $this->createMock(ClientWrapper::class),
            $this->createMock(LoggerInterface::class),
            $dataStorage,
            $this->createMock(FileStagingInterface::class),
            $this->createMock(InputTableStateList::class),
            $destination,
            FileFormat::Json,
        );

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

        $reflection = new ReflectionClass($local);
        $method = $reflection->getMethod('getDataFilePath');
        $method->setAccessible(true);

        $result = $method->invoke($local, $tableOptions);

        $this->assertSame($expected, $result);
    }
}
