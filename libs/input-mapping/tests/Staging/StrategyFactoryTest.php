<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Staging;

use Keboola\InputMapping\File\Strategy\Local as FileLocal;
use Keboola\InputMapping\Staging\StrategyFactory;
use Keboola\InputMapping\State\InputFileStateList;
use Keboola\InputMapping\State\InputTableStateList;
use Keboola\InputMapping\Table\Strategy\ABS as TableAbs;
use Keboola\InputMapping\Table\Strategy\BigQuery as TableBigquery;
use Keboola\InputMapping\Table\Strategy\Local as TableLocal;
use Keboola\InputMapping\Table\Strategy\S3 as TableS3;
use Keboola\InputMapping\Table\Strategy\Snowflake as TableSnowflake;
use Keboola\StagingProvider\Staging\File\FileFormat;
use Keboola\StagingProvider\Staging\File\FileStagingInterface;
use Keboola\StagingProvider\Staging\StagingProvider;
use Keboola\StagingProvider\Staging\StagingType;
use Keboola\StagingProvider\Staging\Workspace\WorkspaceStagingInterface;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class StrategyFactoryTest extends TestCase
{
    private const STAGING_MAP = [
        StagingType::Local->value => [
            'fileStrategy' => FileLocal::class,
            'tableStrategy' => TableLocal::class,
            'tableDataStagingClass' => FileStagingInterface::class,
        ],
        StagingType::S3->value => [
            'fileStrategy' => FileLocal::class,
            'tableStrategy' => TableS3::class,
            'tableDataStagingClass' => FileStagingInterface::class,
        ],
        StagingType::Abs->value => [
            'fileStrategy' => FileLocal::class,
            'tableStrategy' => TableAbs::class,
            'tableDataStagingClass' => FileStagingInterface::class,
        ],
        StagingType::WorkspaceSnowflake->value => [
            'fileStrategy' => FileLocal::class,
            'tableStrategy' => TableSnowflake::class,
            'tableDataStagingClass' => WorkspaceStagingInterface::class,
        ],
        StagingType::WorkspaceBigquery->value => [
            'fileStrategy' => FileLocal::class,
            'tableStrategy' => TableBigquery::class,
            'tableDataStagingClass' => WorkspaceStagingInterface::class,
        ],
    ];

    public function testAllStagingTypesAreCovered(): void
    {
        $missingStagingTypes = array_diff(
            array_map(fn(StagingType $t) => $t->value, StagingType::cases()),
            array_keys(self::STAGING_MAP),
        );

        self::assertSame(
            [
                StagingType::None->value,
            ],
            $missingStagingTypes,
            'Not all staging types are covered by the test',
        );
    }

    public static function provideFileInputStrategyMapping(): iterable
    {
        foreach (self::STAGING_MAP as $stagingType => $strategyConfig) {
            yield $stagingType => [
                'stagingType' => StagingType::from($stagingType),
                'expectedStrategyClass' => $strategyConfig['fileStrategy'],
            ];
        }
    }

    /**
     * @param class-string $expectedStrategyClass
     * @dataProvider provideFileInputStrategyMapping
     */
    public function testGetFileInputStrategy(StagingType $stagingType, string $expectedStrategyClass): void
    {
        $dataStaging = $this->createMock(FileStagingInterface::class);
        $metadataStaging = $this->createMock(FileStagingInterface::class);

        $stagingProvider = $this->createMock(StagingProvider::class);
        $stagingProvider->method('getStagingType')->willReturn($stagingType);
        $stagingProvider->expects(self::once())->method('getFileDataStaging')->willReturn($dataStaging);
        $stagingProvider->expects(self::once())->method('getFileMetadataStaging')->willReturn($metadataStaging);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $logger = $this->createMock(LoggerInterface::class);
        $fileFormat = FileFormat::Yaml;

        $fileStateList = new InputFileStateList([]);

        $factory = new StrategyFactory(
            $stagingProvider,
            $clientWrapper,
            $logger,
            $fileFormat,
        );

        $strategy = $factory->getFileInputStrategy($fileStateList);

        self::assertInstanceOf($expectedStrategyClass, $strategy);
        self::assertEquals(
            new $expectedStrategyClass(
                $clientWrapper,
                $logger,
                $dataStaging,
                $metadataStaging,
                $fileStateList,
                $fileFormat,
            ),
            $strategy,
        );
    }

    public static function provideTableInputStrategyMapping(): iterable
    {
        foreach (self::STAGING_MAP as $stagingType => $strategyConfig) {
            yield $stagingType => [
                'stagingType' => StagingType::from($stagingType),
                'expectedDataStagingClass' => $strategyConfig['tableDataStagingClass'],
                'expectedStrategyClass' => $strategyConfig['tableStrategy'],
            ];
        }
    }

    /**
     * @param class-string $expectedDataStagingClass
     * @param class-string $expectedStrategyClass
     * @dataProvider provideTableInputStrategyMapping
     */
    public function testGetTableInputStrategy(
        StagingType $stagingType,
        string $expectedDataStagingClass,
        string $expectedStrategyClass,
    ): void {
        $dataStaging = $this->createMock($expectedDataStagingClass);
        $metadataStaging = $this->createMock(FileStagingInterface::class);

        $stagingProvider = $this->createMock(StagingProvider::class);
        $stagingProvider->method('getStagingType')->willReturn($stagingType);
        $stagingProvider->expects(self::once())->method('getTableDataStaging')->willReturn($dataStaging);
        $stagingProvider->expects(self::once())->method('getTableMetadataStaging')->willReturn($metadataStaging);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $logger = $this->createMock(LoggerInterface::class);
        $fileFormat = FileFormat::Yaml;

        $tableStateList = new InputTableStateList([]);

        $factory = new StrategyFactory(
            $stagingProvider,
            $clientWrapper,
            $logger,
            $fileFormat,
        );

        $strategy = $factory->getTableInputStrategy('destination', $tableStateList);

        self::assertInstanceOf($expectedStrategyClass, $strategy);
        self::assertEquals(
            new $expectedStrategyClass(
                $clientWrapper,
                $logger,
                $dataStaging,
                $metadataStaging,
                $tableStateList,
                'destination',
                $fileFormat,
            ),
            $strategy,
        );
    }
}
