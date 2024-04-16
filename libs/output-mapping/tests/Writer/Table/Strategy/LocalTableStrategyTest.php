<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table\Strategy;

use Generator;
use Keboola\InputMapping\Staging\ProviderInterface;
use Keboola\OutputMapping\Mapping\MappingFromProcessedConfiguration;
use Keboola\OutputMapping\Tests\AbstractTestCase;
use Keboola\OutputMapping\Writer\Table\Strategy\LocalTableStrategy;
use Psr\Log\NullLogger;
use SplFileInfo;
use Symfony\Component\Filesystem\Filesystem;

class LocalTableStrategyTest extends AbstractTestCase
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

        $strategy = new LocalTableStrategy(
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

        $strategy = new LocalTableStrategy(
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
