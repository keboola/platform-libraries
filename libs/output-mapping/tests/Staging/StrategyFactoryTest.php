<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Staging;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Staging\StrategyFactory;
use Keboola\OutputMapping\Writer\File\Strategy\Local as FileLocal;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy as TableLocal;
use Keboola\OutputMapping\Writer\Table\Strategy\SqlWorkspaceTableStrategy as TableWorkspace;
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
        StagingType::WorkspaceSnowflake->value => [
            'fileStrategy' => FileLocal::class,
            'tableStrategy' => TableWorkspace::class,
            'tableDataStagingClass' => WorkspaceStagingInterface::class,
        ],
        StagingType::WorkspaceBigquery->value => [
            'fileStrategy' => FileLocal::class,
            'tableStrategy' => TableWorkspace::class,
            'tableDataStagingClass' => WorkspaceStagingInterface::class,
        ],
    ];

    public static function provideFileOutputStrategyMapping(): iterable
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
     * @dataProvider provideFileOutputStrategyMapping
     */
    public function testGetFileOutputStrategy(StagingType $stagingType, string $expectedStrategyClass): void
    {
        $dataStaging = $this->createMock(FileStagingInterface::class);
        $metadataStaging = $this->createMock(FileStagingInterface::class);

        $stagingProvider = $this->createMock(StagingProvider::class);
        $stagingProvider->method('getStagingType')->willReturn($stagingType);
        $stagingProvider->expects(self::once())->method('getFileDataStaging')->willReturn($dataStaging);
        $stagingProvider->expects(self::once())->method('getFileMetadataStaging')->willReturn($metadataStaging);
        $stagingProvider->expects(self::never())->method('getTableDataStaging');
        $stagingProvider->expects(self::never())->method('getTableMetadataStaging');

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $logger = $this->createMock(LoggerInterface::class);
        $fileFormat = FileFormat::Yaml;

        $factory = new StrategyFactory(
            $stagingProvider,
            $clientWrapper,
            $logger,
            $fileFormat,
        );

        $strategy = $factory->getFileOutputStrategy();

        self::assertInstanceOf($expectedStrategyClass, $strategy);
        self::assertEquals(
            new $expectedStrategyClass(
                $clientWrapper,
                $logger,
                $dataStaging,
                $metadataStaging,
                $fileFormat,
            ),
            $strategy,
        );
    }

    public static function provideTableOutputStrategyMapping(): iterable
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
     * @dataProvider provideTableOutputStrategyMapping
     */
    public function testGetTableOutputStrategy(
        StagingType $stagingType,
        string $expectedDataStagingClass,
        string $expectedStrategyClass,
    ): void {
        $dataStaging = $this->createMock($expectedDataStagingClass);
        $metadataStaging = $this->createMock(FileStagingInterface::class);

        $stagingProvider = $this->createMock(StagingProvider::class);
        $stagingProvider->method('getStagingType')->willReturn($stagingType);
        $stagingProvider->expects(self::never())->method('getFileDataStaging');
        $stagingProvider->expects(self::never())->method('getFileMetadataStaging');
        $stagingProvider->expects(self::once())->method('getTableDataStaging')->willReturn($dataStaging);
        $stagingProvider->expects(self::once())->method('getTableMetadataStaging')->willReturn($metadataStaging);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $logger = $this->createMock(LoggerInterface::class);
        $fileFormat = FileFormat::Yaml;

        $factory = new StrategyFactory(
            $stagingProvider,
            $clientWrapper,
            $logger,
            $fileFormat,
        );

        $strategy = $factory->getTableOutputStrategy(isFailedJob: true);

        self::assertInstanceOf($expectedStrategyClass, $strategy);
        self::assertEquals(
            new $expectedStrategyClass(
                $clientWrapper,
                $logger,
                $dataStaging,
                $metadataStaging,
                $fileFormat,
                isFailedJob: true,
            ),
            $strategy,
        );
    }

    public static function provideUnsupportedStagingTypes(): iterable
    {
        $supportedTypes = array_keys(self::STAGING_MAP);
        foreach (StagingType::cases() as $stagingType) {
            if (in_array($stagingType->value, $supportedTypes)) {
                continue;
            }

            yield $stagingType->value => [
                'stagingType' => $stagingType,
            ];
        }
    }

    /** @dataProvider provideUnsupportedStagingTypes */
    public function testGetFileOutputStrategyWithUnsupportedStaging(StagingType $stagingType): void
    {
        $stagingProvider = $this->createMock(StagingProvider::class);
        $stagingProvider->method('getStagingType')->willReturn($stagingType);
        $stagingProvider->expects(self::never())->method('getFileDataStaging');
        $stagingProvider->expects(self::never())->method('getFileMetadataStaging');
        $stagingProvider->expects(self::never())->method('getTableDataStaging');
        $stagingProvider->expects(self::never())->method('getTableMetadataStaging');

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $logger = $this->createMock(LoggerInterface::class);
        $fileFormat = FileFormat::Yaml;

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(sprintf(
            'Staging type "%s" is not supported for file output.',
            $stagingType->value,
        ));

        new StrategyFactory(
            $stagingProvider,
            $clientWrapper,
            $logger,
            $fileFormat,
        );
    }
}
