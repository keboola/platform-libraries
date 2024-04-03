<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use InvalidArgumentException;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Writer\Table\MappingResolver\WorkspaceMappingResolver;
use Keboola\OutputMapping\Writer\Table\Source\AbsWorkspaceItemSourceFactory;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use Keboola\OutputMapping\Writer\Table\Strategy\AbsWorkspaceTableStrategy;
use Keboola\StorageApiBranch\ClientWrapper;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use ReflectionProperty;

class AbsWorkspaceTableStrategyTest extends TestCase
{
    public function testPrepareLoadTaskOptions(): void
    {
        $source = $this->createMock(WorkspaceItemSource::class);
        $source->expects(self::once())
            ->method('getWorkspaceId')
            ->willReturn('123456')
        ;
        $source->expects(self::once())
            ->method('getDataObject')
            ->willReturn('myTable')
        ;

        $strategy = new AbsWorkspaceTableStrategy(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $this->createMock(ProviderInterface::class),
            'json',
            false,
        );

        self::assertSame(
            [
                'dataWorkspaceId' => '123456',
                'dataObject' => 'myTable',
            ],
            $strategy->prepareLoadTaskOptions($source, []),
        );
    }

    public function testPrepareLoadTaskOptionsFailsOnNonWorkspaceItemSource(): void
    {
        $strategy = new AbsWorkspaceTableStrategy(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $this->createMock(ProviderInterface::class),
            'json',
            false,
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument $source is expected to be instance of '
            . 'Keboola\OutputMapping\Writer\Table\WorkspaceItemSource');

        $strategy->prepareLoadTaskOptions($this->createMock(SourceInterface::class), []);
    }

    public function testGetters(): void
    {
        $dataStorageProvider = $this->createMock(ProviderInterface::class);

        $metadataStorageProvider = $this->createMock(ProviderInterface::class);
        $metadataStorageProvider->expects(self::once())
            ->method('getPath')
            ->willReturn('test')
        ;

        $strategy = new AbsWorkspaceTableStrategy(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            $dataStorageProvider,
            $metadataStorageProvider,
            'json',
            false,
        );

        self::assertSame($dataStorageProvider, $strategy->getDataStorage());
        self::assertSame($metadataStorageProvider, $strategy->getMetadataStorage());

        $mappingResolver = $strategy->getMappingResolver();
        self::assertInstanceOf(WorkspaceMappingResolver::class, $mappingResolver);

        $reflection = new ReflectionProperty($mappingResolver, 'path');
        self::assertSame('test', $reflection->getValue($mappingResolver));

        $reflection = new ReflectionProperty($mappingResolver, 'sourceFactory');
        $sourceFactory = $reflection->getValue($mappingResolver);
        self::assertIsObject($sourceFactory);
        self::assertInstanceOf(AbsWorkspaceItemSourceFactory::class, $sourceFactory);

        $reflection = new ReflectionProperty($sourceFactory, 'dataStorage');
        $dataStorage = $reflection->getValue($sourceFactory);
        self::assertSame($dataStorageProvider, $dataStorage);
    }
}
