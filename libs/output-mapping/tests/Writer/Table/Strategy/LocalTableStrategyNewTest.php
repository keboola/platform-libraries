<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use Generator;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\OutputOperationException;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\SourcesValidator\LocalSourcesValidator;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Writer\FileItem;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategyNew;
use Keboola\StorageApiBranch\ClientWrapper;
use Psr\Log\NullLogger;
use SplFileInfo;
use Symfony\Component\Filesystem\Filesystem;

class LocalTableStrategyNewTest extends AbstractTestCase
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
        $file = $this->temp->createFile('test.csv');

        $sourceMock = $this->createSourceMock($file, false, $expectedTags);

        $strategy = new LocalTableStrategyNew(
            $this->clientWrapper,
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $this->createMock(ProviderInterface::class),
            'json',
            false,
        );

        $loadTaskOptions = $strategy->prepareLoadTaskOptions($sourceMock);
        self::assertCount(3, array_keys($loadTaskOptions));

        self::assertArrayHasKey('delimiter', $loadTaskOptions);
        self::assertSame(';', $loadTaskOptions['delimiter']);
        self::assertArrayHasKey('enclosure', $loadTaskOptions);
        self::assertSame('|', $loadTaskOptions['enclosure']);

        self::assertArrayHasKey('dataFileId', $loadTaskOptions);

        $file = $this->clientWrapper->getTableAndFileStorageClient()->getFile($loadTaskOptions['dataFileId']);
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
        $file = new SplFileInfo(sprintf('%s/myTable', $this->temp->getTmpFolder()));
        (new Filesystem())->mkdir($file->getPathname());

        $sourceMock = $this->createSourceMock($file, true, $expectedTags);

        $strategy = new LocalTableStrategyNew(
            $this->clientWrapper,
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $this->createMock(ProviderInterface::class),
            'json',
            false,
        );

        $loadTaskOptions = $strategy->prepareLoadTaskOptions($sourceMock);
        self::assertCount(3, array_keys($loadTaskOptions));

        self::assertArrayHasKey('delimiter', $loadTaskOptions);
        self::assertSame(';', $loadTaskOptions['delimiter']);
        self::assertArrayHasKey('enclosure', $loadTaskOptions);
        self::assertSame('|', $loadTaskOptions['enclosure']);

        self::assertArrayHasKey('dataFileId', $loadTaskOptions);

        $file = $this->clientWrapper->getTableAndFileStorageClient()->getFile($loadTaskOptions['dataFileId']);
        self::assertArrayHasKey('isSliced', $file);
        self::assertTrue($file['isSliced']);
        self::assertArrayHasKey('name', $file);
        self::assertSame('mytable.gz', $file['name']);
        self::assertArrayHasKey('tags', $file);
        self::assertSame($expectedTags, $file['tags']);
    }

    public function testListManifest(): void
    {
        $metadataStore = $this->createMock(ProviderInterface::class);
        $metadataStore->method('getPath')->willReturn($this->temp->getTmpFolder());

        $strategy = new LocalTableStrategyNew(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $metadataStore,
            'json',
            false,
        );

        for ($i = 0; $i < 3; $i++) {
            $manifestFile = $this->temp->getTmpFolder() . '/file_' . $i . '.csv.manifest';
            file_put_contents($manifestFile, '');
        }

        $manifests = $strategy->listManifests('/');
        $this->assertIsArray($manifests);
        self::assertCount(3, $manifests);
    }

    public function testInvalidListManifest(): void
    {
        $strategy = new LocalTableStrategyNew(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $this->createMock(ProviderInterface::class),
            'json',
            false,
        );

        $this->expectException(OutputOperationException::class);
        $this->expectExceptionMessage('Failed to list files: "The "dir-unexist" directory does not exist.".');
        $strategy->listManifests('/dir-unexist');
    }

    public function testListSources(): void
    {
        $dataStore = $this->createMock(ProviderInterface::class);
        $dataStore->method('getPath')->willReturn($this->temp->getTmpFolder());

        $strategy = new LocalTableStrategyNew(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            $dataStore,
            $dataStore,
            'json',
            false,
        );

        for ($i = 0; $i < 3; $i++) {
            $manifestFile = $this->temp->getTmpFolder() . '/upload/file_' . $i . '.csv';
            file_put_contents($manifestFile, '');
        }

        $sources = $strategy->listSources('/upload', []);
        self::assertIsArray($sources);
        self::assertCount(3, $sources);
        foreach ($sources as $source) {
            self::assertInstanceOf(FileItem::class, $source);
        }
    }

    public function testReadFileManifest(): void
    {
        $metadataStore = $this->createMock(ProviderInterface::class);
        $metadataStore->method('getPath')->willReturn($this->temp->getTmpFolder());

        $strategy = new LocalTableStrategyNew(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $metadataStore,
            'json',
            false,
        );

        $manifestFile = $this->temp->getTmpFolder() . '/file.csv.manifest';
        file_put_contents($manifestFile, json_encode([
            'columns' => [
                'col1',
                'col2',
            ],
        ]));

        $result = $strategy->readFileManifest(
            new FileItem('file.csv.manifest', '', 'file.csv.manifest', false),
        );

        self::assertIsArray($result);
        self::assertCount(2, $result['columns']);
    }

    public function testReadInvalidFileManifest(): void
    {
        $metadataStore = $this->createMock(ProviderInterface::class);
        $metadataStore->method('getPath')->willReturn($this->temp->getTmpFolder());

        $strategy = new LocalTableStrategyNew(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $metadataStore,
            'json',
            false,
        );

        $manifestFile = $this->temp->getTmpFolder() . '/file.csv.manifest';
        file_put_contents($manifestFile, 'invalidJson');

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(sprintf(
            'Failed to parse manifest file "%s//file.csv.manifest" as "json": Syntax error',
            $this->temp->getTmpFolder(),
        ));
        $strategy->readFileManifest(
            new FileItem('file.csv.manifest', '', 'file.csv.manifest', false),
        );
    }

    public function testReadFileManifestNotFound(): void
    {
        $strategy = new LocalTableStrategyNew(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $this->createMock(ProviderInterface::class),
            'json',
            false,
        );

        $manifest = $strategy->readFileManifest(
            new FileItem('file.csv.manifest', '', 'file.csv.manifest', false),
        );

        $this->assertEmpty($manifest);
    }

    public function testGetSourcesValidator(): void
    {
        $strategy = new LocalTableStrategyNew(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $this->createMock(ProviderInterface::class),
            'json',
            false,
        );

        $result = $strategy->getSourcesValidator();
        $this->assertInstanceOf(LocalSourcesValidator::class, $result);
    }

    public function testHasSlicerAlwaysTrue(): void
    {
        $strategy = new LocalTableStrategyNew(
            $this->createMock(ClientWrapper::class),
            new NullLogger(),
            $this->createMock(ProviderInterface::class),
            $this->createMock(ProviderInterface::class),
            'json',
            false,
        );

        $result = $strategy->hasSlicer();
        $this->assertTrue($result);
    }

    public function testSliceFiles(): void
    {
        $this->temp->createFile('file.csv');
        for ($rows= 0; $rows < 2000000; $rows++) {
            file_put_contents(
                $this->temp->getTmpFolder() . '/file.csv',
                "longlonglongrow{$rows}, abcdefghijklnoppqrstuvwxyz\n",
                FILE_APPEND,
            );
        }

        $this->temp->createFile('smallFile.csv');
        for ($rows= 0; $rows < 2000000; $rows++) {
            file_put_contents(
                $this->temp->getTmpFolder() . '/smallFile.csv',
                "{$rows}\n",
                FILE_APPEND,
            );
        }

        $strategy = new LocalTableStrategyNew(
            $this->createMock(ClientWrapper::class),
            $this->testLogger,
            $this->createMock(ProviderInterface::class),
            $this->createMock(ProviderInterface::class),
            'json',
            false,
        );

        $combinedMappingItem = $this->createMock(
            MappingFromRawConfigurationAndPhysicalDataWithManifest::class,
        );
        $combinedMappingItem
            ->method('getPathName')
            ->willReturn($this->temp->getTmpFolder() . '/file.csv');
        $combinedMappingItem
            ->method('getSourceName')
            ->willReturn('file.csv');
        $combinedMappingItem
            ->method('getPathNameManifest')
            ->willReturn($this->temp->getTmpFolder() . '/file.csv.manifest');

        $secondCombinedMappingItem = $this->createMock(
            MappingFromRawConfigurationAndPhysicalDataWithManifest::class,
        );
        $secondCombinedMappingItem
            ->method('getPathName')
            ->willReturn($this->temp->getTmpFolder() . '/smallFile.csv');
        $secondCombinedMappingItem
            ->method('getSourceName')
            ->willReturn('smallFile.csv');
        $secondCombinedMappingItem
            ->method('getPathNameManifest')
            ->willReturn($this->temp->getTmpFolder() . '/smallFile.csv.manifest');

        $strategy->sliceFiles([$combinedMappingItem, $secondCombinedMappingItem]);

        self::assertTrue($this->testHandler->hasInfo('Slicing table "file.csv".'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Table "file.csv" sliced: in/out: 1 / 1 slices'));
        self::assertTrue($this->testHandler->hasInfoThatContains('Skipping table "smallFile.csv": table size'));
    }

    private function createSourceMock(SplFileInfo $file, bool $isSliced, array $tags): MappingFromProcessedConfiguration
    {
        $source = $this->createMock(MappingFromProcessedConfiguration::class);

        $source->expects(self::once())->method('isSliced')->willReturn($isSliced);
        $source->expects(self::once())->method('getPathName')->willReturn($file->getPathname());
        $source->method('getSourceName')->willReturn($file->getFilename());
        $source->expects(self::once())->method('getTags')->willReturn($tags);
        $source->expects(self::once())->method('getDelimiter')->willReturn(';');
        $source->expects(self::once())->method('getEnclosure')->willReturn('|');
        return $source;
    }
}
