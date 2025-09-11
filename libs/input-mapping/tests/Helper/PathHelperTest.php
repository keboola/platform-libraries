<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Generator;
use Keboola\InputMapping\Helper\PathHelper;
use Keboola\InputMapping\Table\Options\RewrittenInputTableOptions;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use PHPUnit\Framework\TestCase;

class PathHelperTest extends TestCase
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
        $metadataStorageMock->expects(self::once())->method('getPath')->willReturn($metadataPath);

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

        $result = PathHelper::getManifestPath($metadataStorageMock, $destination, $tableOptions);

        self::assertSame($expected, $result);
    }

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

        yield 'data path without leading slash' => [
            'dataPath' => 'tmp/data',
            'destination' => 'destination-folder',
            'tableSource' => 'test-table',
            'tableDestination' => 'my-table',
            'expected' => 'tmp/data/destination-folder/my-table',
        ];

        yield 'complex paths with multiple segments' => [
            'dataPath' => '/var/tmp/keboola/data',
            'destination' => 'input/tables',
            'tableSource' => 'bucket.table-name',
            'tableDestination' => 'transformed-table',
            'expected' => '/var/tmp/keboola/data/input/tables/transformed-table',
        ];

        yield 'data path with multiple trailing slashes' => [
            'dataPath' => '/tmp/data///',
            'destination' => 'destination-folder',
            'tableSource' => 'test-table',
            'tableDestination' => 'my-table',
            'expected' => '/tmp/data/destination-folder/my-table',
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
        $dataStorageMock = $this->createMock(FileStagingInterface::class);
        $dataStorageMock->expects(self::once())->method('getPath')->willReturn($dataPath);

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

        $result = PathHelper::getDataFilePath($dataStorageMock, $destination, $tableOptions);

        self::assertSame($expected, $result);
    }

    public static function ensurePathDelimiterDataProvider(): Generator
    {
        yield 'path without trailing slash' => [
            'input' => '/tmp/data',
            'expected' => '/tmp/data/',
        ];

        yield 'path with trailing slash' => [
            'input' => '/tmp/data/',
            'expected' => '/tmp/data/',
        ];

        yield 'path with multiple trailing slashes' => [
            'input' => '/tmp/data///',
            'expected' => '/tmp/data/',
        ];

        yield 'path with backslashes' => [
            'input' => '/tmp/data\\\\',
            'expected' => '/tmp/data/',
        ];

        yield 'empty path' => [
            'input' => '',
            'expected' => '/',
        ];

        yield 'root path' => [
            'input' => '/',
            'expected' => '/',
        ];
    }

    /**
     * @dataProvider ensurePathDelimiterDataProvider
     */
    public function testEnsurePathDelimiter(string $input, string $expected): void
    {
        $result = PathHelper::ensurePathDelimiter($input);
        self::assertSame($expected, $result);
    }
}
