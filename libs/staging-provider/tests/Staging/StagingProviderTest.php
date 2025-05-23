<?php

declare(strict_types=1);

namespace Keboola\StagingProvider\Tests\Staging;

use InvalidArgumentException;
use Keboola\StagingProvider\Staging\File\LocalStaging;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStaging;
use PHPUnit\Framework\TestCase;

class StagingProviderTest extends TestCase
{
    public static function provideDiskStagingTypes(): iterable
    {
        yield 'local' => [StagingType::Local];
        yield 's3' => [StagingType::S3];
        yield 'abs' => [StagingType::Abs];
    }

    /** @dataProvider provideDiskStagingTypes */
    public function testFileStagingIsReturnedForDiskStaging(StagingType $stagingType): void
    {
        $stagingDirPath = '/tmp/random/data';
        $provider = new StagingProvider(
            $stagingType,
            $stagingDirPath,
            null,
        );

        self::assertSame($stagingType, $provider->getStagingType());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getFileDataStaging());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getFileMetadataStaging());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getTableDataStaging());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getTableMetadataStaging());
    }

    public static function provideWorkspaceStagingTypes(): iterable
    {
        yield 'workspace-snowflake' => [StagingType::WorkspaceSnowflake];
        yield 'workspace-bigquery' => [StagingType::WorkspaceBigquery];
    }

    /** @dataProvider provideWorkspaceStagingTypes */
    public function testWorkspaceStagingIsReturnedForWorkspaceStaging(StagingType $stagingType): void
    {
        $stagingDirPath = '/tmp/random/data';
        $workspaceId = '123';
        $provider = new StagingProvider(
            $stagingType,
            $stagingDirPath,
            $workspaceId,
        );

        self::assertSame($stagingType, $provider->getStagingType());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getFileDataStaging());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getFileMetadataStaging());
        self::assertEquals(new WorkspaceStaging('123'), $provider->getTableDataStaging());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getTableMetadataStaging());
    }

    public function testCreateForStagingTypeWithDiskStaging(): void
    {
        $stagingDirPath = '/tmp/random/data';
        $provider = new StagingProvider(
            StagingType::Local,
            $stagingDirPath,
            null,
        );

        self::assertSame(StagingType::Local, $provider->getStagingType());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getFileDataStaging());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getFileMetadataStaging());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getTableDataStaging());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getTableMetadataStaging());
    }

    public function testCreateForStagingTypeWithWorkspaceStaging(): void
    {
        $stagingDirPath = '/tmp/random/data';
        $workspaceId = '123';
        $provider = new StagingProvider(
            StagingType::WorkspaceSnowflake,
            $stagingDirPath,
            $workspaceId,
        );

        self::assertSame(StagingType::WorkspaceSnowflake, $provider->getStagingType());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getFileDataStaging());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getFileMetadataStaging());
        self::assertEquals(new WorkspaceStaging('123'), $provider->getTableDataStaging());
        self::assertEquals(new LocalStaging('/tmp/random/data'), $provider->getTableMetadataStaging());
    }

    public function testErrorWhenWorkspaceIdProvidedForDiskStaging(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Staging workspace ID must be configured (only) with workspace staging.');

        new StagingProvider(
            StagingType::Local,
            '/tmp/random/data',
            '123',
        );
    }

    public function testErrorWhenWorkspaceIdNotProvidedForWorkspaceStaging(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Staging workspace ID must be configured (only) with workspace staging.');

        new StagingProvider(
            StagingType::WorkspaceSnowflake,
            '/tmp/random/data',
            null,
        );
    }
}
