<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use Generator;
use InvalidArgumentException;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Writer\SourceInterface;
use Keboola\OutputMapping\Writer\Strategy\LocalTableStrategy;
use Keboola\OutputMapping\Writer\Table\MappingResolver\LocalMappingResolver;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use ReflectionProperty;
use SplFileInfo;
use Symfony\Component\Filesystem\Filesystem;

class LocalTableStrategyTest extends TestCase
{
    public function prepareLoadTaskOptionsProvider(): Generator
    {
        yield 'minimal options' => [
            'config' => [
                'delimiter' => ';',
                'enclosure' => '|',
            ],
            'expectedTags' => [],
        ];
        yield 'with tags' => [
            'config' => [
                'delimiter' => ';',
                'enclosure' => '|',
                'tags' => ['test-tag'],
            ],
            'expectedTags' => ['test-tag'],
        ];
    }

    /**
     * @dataProvider prepareLoadTaskOptionsProvider
     */
    public function testPrepareLoadTaskOptions(array $config, array $expectedTags): void
    {
        $temp = new Temp();
        $file = $temp->createFile('test.csv');

        $clientWrapper = $this->createStorageClientWrapper();
        $localFileMock = $this->createLocalFileSourceMock($file, false);

        $strategy = new LocalTableStrategy(
            $clientWrapper,
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $this->createMock(ProviderInterface::class),
            'json',
            false,
        );

        $loadTaskOptions = $strategy->prepareLoadTaskOptions($localFileMock, $config);
        self::assertCount(3, array_keys($loadTaskOptions));

        self::assertArrayHasKey('delimiter', $loadTaskOptions);
        self::assertSame(';', $loadTaskOptions['delimiter']);
        self::assertArrayHasKey('enclosure', $loadTaskOptions);
        self::assertSame('|', $loadTaskOptions['enclosure']);

        self::assertArrayHasKey('dataFileId', $loadTaskOptions);

        $file = $clientWrapper->getTableAndFileStorageClient()->getFile($loadTaskOptions['dataFileId']);
        self::assertArrayHasKey('isSliced', $file);
        self::assertFalse($file['isSliced']);
        self::assertArrayHasKey('name', $file);
        self::assertSame('test.csv.gz', $file['name']);
        self::assertArrayHasKey('tags', $file);
        self::assertSame($expectedTags, $file['tags']);
    }

    /**
     * @dataProvider prepareLoadTaskOptionsProvider
     */
    public function testPrepareLoadTaskOptionsForSlicedFile(array $config, array $expectedTags): void
    {
        $temp = new Temp();
        $file = new SplFileInfo(sprintf('%s/myTable', $temp->getTmpFolder()));

        (new Filesystem())->mkdir($file->getPathname());

        $clientWrapper = $this->createStorageClientWrapper();
        $localFileMock = $this->createLocalFileSourceMock($file, true);

        $strategy = new LocalTableStrategy(
            $clientWrapper,
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $this->createMock(ProviderInterface::class),
            'json',
            false,
        );

        $loadTaskOptions = $strategy->prepareLoadTaskOptions($localFileMock, $config);
        self::assertCount(3, array_keys($loadTaskOptions));

        self::assertArrayHasKey('delimiter', $loadTaskOptions);
        self::assertSame(';', $loadTaskOptions['delimiter']);
        self::assertArrayHasKey('enclosure', $loadTaskOptions);
        self::assertSame('|', $loadTaskOptions['enclosure']);

        self::assertArrayHasKey('dataFileId', $loadTaskOptions);

        $file = $clientWrapper->getTableAndFileStorageClient()->getFile($loadTaskOptions['dataFileId']);
        self::assertArrayHasKey('isSliced', $file);
        self::assertTrue($file['isSliced']);
        self::assertArrayHasKey('name', $file);
        self::assertSame('mytable.gz', $file['name']);
        self::assertArrayHasKey('tags', $file);
        self::assertSame($expectedTags, $file['tags']);
    }

    public function testPrepareLoadTaskOptionsFaisOnNonWorkspaceItemSource(): void
    {
        $strategy = new LocalTableStrategy(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $this->createMock(ProviderInterface::class),
            'json',
            false,
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Argument $source is expected to be instance of '
            . 'Keboola\OutputMapping\Writer\Table\Source\LocalFileSource');

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

        $strategy = new LocalTableStrategy(
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
        self::assertInstanceOf(LocalMappingResolver::class, $mappingResolver);

        $reflection = new ReflectionProperty($mappingResolver, 'path');
        self::assertSame('test', $reflection->getValue($mappingResolver));
    }

    private function createStorageClientWrapper(): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(
                (string) getenv('STORAGE_API_URL'),
                (string) getenv('STORAGE_API_TOKEN'),
            ),
        );
    }

    private function createLocalFileSourceMock(SplFileInfo $file, bool $isSliced): LocalFileSource
    {
        $localFileMock = $this->createMock(LocalFileSource::class);
        $localFileMock->expects(self::once())
            ->method('isSliced')
            ->willReturn($isSliced)
        ;
        $localFileMock->expects(self::once())
            ->method('getFile')
            ->willReturn($file)
        ;
        return $localFileMock;
    }
}
