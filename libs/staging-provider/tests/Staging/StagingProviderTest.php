<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Staging;

use Keboola\StagingProvider\Exception\NoStagingAvailableException;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\File\LocalStaging;
use Keboola\StagingProvider\Staging\StagingClass;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStaging;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use PHPUnit\Framework\TestCase;

class StagingProviderTest extends TestCase
{
    public static function provideFileStagingTypes(): iterable
    {
        yield 'local' => [StagingType::Local];
        yield 's3' => [StagingType::S3];
        yield 'abs' => [StagingType::Abs];
    }

    /** @dataProvider provideFileStagingTypes */
    public function testFileStagingIsReturnedForFileStaging(StagingType $stagingType): void
    {
        $localStaging = new LocalStaging('/tmp/random/data');
        $workspaceStaging = new WorkspaceStaging('123');

        $provider = new StagingProvider(
            $stagingType,
            $workspaceStaging,
            $localStaging,
        );

        self::assertSame($stagingType, $provider->getStagingType());
        self::assertSame($localStaging, $provider->getFileDataStaging());
        self::assertSame($localStaging, $provider->getFileMetadataStaging());
        self::assertSame($localStaging, $provider->getTableDataStaging());
        self::assertSame($localStaging, $provider->getTableMetadataStaging());
    }

    public static function provideWorkspaceStagingTypes(): iterable
    {
        yield 'workspace-snowflake' => [StagingType::WorkspaceSnowflake];
        yield 'workspace-bigquery' => [StagingType::WorkspaceBigquery];
    }

    /** @dataProvider provideWorkspaceStagingTypes */
    public function testWorkspaceStagingIsReturnedForWorkspaceStaging(StagingType $stagingType): void
    {
        $localStaging = new LocalStaging('/tmp/random/data');
        $workspaceStaging = new WorkspaceStaging('123');

        $provider = new StagingProvider(
            $stagingType,
            $workspaceStaging,
            $localStaging,
        );

        self::assertSame($stagingType, $provider->getStagingType());
        self::assertSame($localStaging, $provider->getFileDataStaging());
        self::assertSame($localStaging, $provider->getFileMetadataStaging());
        self::assertSame($workspaceStaging, $provider->getTableDataStaging());
        self::assertSame($localStaging, $provider->getTableMetadataStaging());
    }

    public function testErrorWhenNoFileDataStagingIsConfigured(): void
    {
        $provider = new StagingProvider(
            StagingType::WorkspaceSnowflake,
            null,
            null,
        );

        $this->expectException(NoStagingAvailableException::class);
        $this->expectExceptionMessage('Undefined file data provider in "WorkspaceSnowflake" staging.');

        $provider->getFileDataStaging();
    }

    public function testErrorWhenNoFileMetadataStagingIsConfigured(): void
    {
        $provider = new StagingProvider(
            StagingType::WorkspaceSnowflake,
            null,
            null,
        );

        $this->expectException(NoStagingAvailableException::class);
        $this->expectExceptionMessage('Undefined file metadata provider in "WorkspaceSnowflake" staging.');

        $provider->getFileMetadataStaging();
    }

    public function testErrorWhenNoTableDataStagingIsConfigured(): void
    {
        $provider = new StagingProvider(
            StagingType::WorkspaceSnowflake,
            null,
            null,
        );

        $this->expectException(NoStagingAvailableException::class);
        $this->expectExceptionMessage('Undefined table data provider in "WorkspaceSnowflake" staging.');

        $provider->getTableDataStaging();
    }

    public function testErrorWhenNoTableMetadataStagingIsConfigured(): void
    {
        $provider = new StagingProvider(
            StagingType::WorkspaceSnowflake,
            null,
            null,
        );

        $this->expectException(NoStagingAvailableException::class);
        $this->expectExceptionMessage('Undefined table metadata provider in "WorkspaceSnowflake" staging.');

        $provider->getTableMetadataStaging();
    }
}
