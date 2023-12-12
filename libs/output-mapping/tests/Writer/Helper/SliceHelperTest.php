<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Generator;
use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Exception\SliceSkippedException;
use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\OutputMapping\Writer\Helper\SliceHelper;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use Keboola\OutputMapping\Writer\Table\Source\SourceInterface;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;
use SplFileInfo;
use Symfony\Component\Filesystem\Exception\FileNotFoundException;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo as FinderSplFileInfo;

class SliceHelperTest extends TestCase
{
    private readonly Temp $temp;
    private readonly TestLogger $logger;

    public function setUp(): void
    {
        parent::setUp();

        $this->temp = new Temp();
        $this->logger = new TestLogger();
    }

    public function testSliceWorkspaceSourceIsNotSupported(): void
    {
        $source = new WorkspaceItemSource(
            'dummy',
            '123',
            'test',
            false,
        );

        $this->expectException(SliceSkippedException::class);
        $this->expectExceptionMessage('Only local files are supported for slicing.');
        (new SliceHelper(new NullLogger()))->sliceFile(new MappingSource($source));
    }

    public function testSliceSlicedSourceIsNotSupported(): void
    {
        $source = new LocalFileSource(new SplFileInfo($this->temp->getTmpFolder()));

        $this->expectException(SliceSkippedException::class);
        $this->expectExceptionMessage('Sliced files are not yet supported.');
        (new SliceHelper(new NullLogger()))->sliceFile(new MappingSource($source));
    }

    public function testSliceEmptyFileSourceIsNotSupported(): void
    {
        $this->expectException(SliceSkippedException::class);
        $this->expectExceptionMessage('Empty files cannot be sliced.');

        (new SliceHelper(new NullLogger()))->sliceFile(
            new MappingSource(
                new LocalFileSource(
                    (new Temp())->createFile('test.csv'),
                ),
            ),
        );
    }

    public function sliceSourceWithSomeMappingOptionsIsNotSupportedProvider(): Generator
    {
        yield 'mapping with csv options - non-default delimiter' => [
            'mapping' => ['delimiter' => ';'],
        ];
        yield 'mapping with csv options - non-default enclosure' => [
            'mapping' => ['enclosure' => '\''],
        ];
        yield 'mapping with columns' => [
            'mapping' => ['columns' => ['Id']],
        ];
    }

    /**
     * @dataProvider sliceSourceWithSomeMappingOptionsIsNotSupportedProvider
     */
    public function testSliceSourceWithMappingHavingCsvOptionsIsNotSupported(
        array $mapping,
    ): void {
        $file = $this->temp->createFile('test.csv');
        file_put_contents($file->getPathname(), '"id","name"');

        $mappingSource = new MappingSource(
            new LocalFileSource($file),
        );
        $mappingSource->setMapping($mapping);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(
            'Params "delimiter", "enclosure" or "columns" '
            . 'specified in mapping are not longer supported.',
        );

        (new SliceHelper(new NullLogger()))->sliceFile($mappingSource);
    }

    public function sliceProvider(): Generator
    {
        yield 'file without manifest' => [
            'originalMappingSource' => $this->createTestMappingSource('test1.csv', new Temp()),
            'expectedData' => '"123","Test Name"' . PHP_EOL,
            'expectedManifestData' => [
                'columns' => ['id', 'name'],
            ],
        ];
        yield 'file with manifest' => [
            'originalMappingSource' => $this->createTestMappingSourceHavingManifest('test2.csv', new Temp()),
            'expectedData' => '"123";"Test Name"' . PHP_EOL,
            'expectedManifestData' => [
                'delimiter' => ';',
                'columns' => ['id', 'name'],
            ],
        ];
        yield 'file without manifest - with default enclosure in mapping' => [
            'originalMappingSource' => $this->createTestMappingSourceHavingMapping(
                'test1.csv',
                new Temp(),
                ['enclosure' => '"'], // not in expectedManifestData - config merge is done after slice in TableWriter
            ),
            'expectedData' => '"123","Test Name"' . PHP_EOL,
            'expectedManifestData' => [
                'columns' => ['id', 'name'],
            ],
        ];
        yield 'file without manifest - with default delimiter in mapping' => [
            'originalMappingSource' => $this->createTestMappingSourceHavingMapping(
                'test1.csv',
                new Temp(),
                ['delimiter' => ','], // not in expectedManifestData - config merge is done after slice in TableWriter
            ),
            'expectedData' => '"123","Test Name"' . PHP_EOL,
            'expectedManifestData' => [
                'columns' => ['id', 'name'],
            ],
        ];
        yield 'file without manifest - with empty columns' => [
            'originalMappingSource' => $this->createTestMappingSourceHavingMapping(
                'test1.csv',
                new Temp(),
                [], // not in expectedManifestData - config merge is done after slice in TableWriter
            ),
            'expectedData' => '"123","Test Name"' . PHP_EOL,
            'expectedManifestData' => [
                'columns' => ['id', 'name'],
            ],
        ];
        yield 'compressed file without manifest' => [
            'originalMappingSource' => $this->createTestGzippedMappingSource('test1.csv', new Temp()),
            'expectedData' => '"123","Test Name"' . PHP_EOL,
            'expectedManifestData' => [
                'columns' => ['id', 'name'],
            ],
        ];
    }

    /**
     * @dataProvider sliceProvider
     */
    public function testSlice(
        MappingSource $originalMappingSource,
        string $expectedData,
        array $expectedManifestData,
    ): void {
        /** @var LocalFileSource $originalSource */
        $originalSource = $originalMappingSource->getSource();
        $mappingSource = (new SliceHelper($this->logger, '3b'))->sliceFile($originalMappingSource);

        // manifest data
        self::assertNotNull($mappingSource->getManifestFile());
        $manifestFilePathName = $mappingSource->getManifestFile()->getPathname();
        self::assertSame(
            $originalSource->getFile()->getPathname() . '.manifest',
            $manifestFilePathName,
        );
        self::assertManifestData($expectedManifestData, $manifestFilePathName);

        // source
        self::assertSource($originalSource, $mappingSource->getSource());

        $dataFiles = FilesHelper::getDataFiles($originalSource->getFile()->getPath());
        self::assertCount(1, $dataFiles);

        /** @var FinderSplFileInfo $slicedDirectory */
        $slicedDirectory = array_shift($dataFiles);

        self::assertSame($originalSource->getFile()->getPathname(), $slicedDirectory->getPathname());
        self::assertCompressedSlicedData($expectedData, $slicedDirectory->getPathname());

        self::assertCount(2, $this->logger->records);
        self::assertTrue($this->logger->hasInfoThatContains(sprintf(
            'Slicing table "%s".',
            $originalMappingSource->getSourceName(),
        )));
        self::assertTrue($this->logger->hasInfoThatContains(sprintf(
            'Table "%s" sliced',
            $originalMappingSource->getSourceName(),
        )));
    }

    public function testSliceSources(): void
    {
        $originalMappingSource = $this->createTestMappingSource('test1.csv', $this->temp);
        /** @var LocalFileSource $originalSource */
        $originalSource = $originalMappingSource->getSource();

        $originalSlicedMappingSource = $this->createTestMappingSourceHavingManifest('test2.csv', $this->temp);
        /** @var LocalFileSource $originalSlicedSource */
        $originalSlicedSource = $originalSlicedMappingSource->getSource();

        $originalMappingSources = [
            $originalMappingSource,
            $originalSlicedMappingSource,
        ];

        $mappingSources = (new SliceHelper($this->logger, '3b'))->sliceSources($originalMappingSources);
        self::assertCount(2, $mappingSources);

        // manifests data
        self::assertNotNull($mappingSources[0]->getManifestFile());
        $manifestFilePathName = $mappingSources[0]->getManifestFile()->getPathname();
        self::assertSame(
            $originalSource->getFile()->getPathname() . '.manifest',
            $manifestFilePathName,
        );
        self::assertManifestData(
            [
                'columns' => ['id', 'name'],
            ],
            $manifestFilePathName,
        );

        self::assertNotNull($mappingSources[1]->getManifestFile());
        $manifestFilePathName = $mappingSources[1]->getManifestFile()->getPathname();
        self::assertSame(
            $originalSlicedSource->getFile()->getPathname() . '.manifest',
            $manifestFilePathName,
        );
        self::assertManifestData(
            [
                'delimiter' => ';',
                'columns' => ['id', 'name'],
            ],
            $manifestFilePathName,
        );

        // sources
        self::assertSource($originalSource, $mappingSources[0]->getSource());
        self::assertSource($originalSlicedSource, $mappingSources[1]->getSource());

        $slicedDirectories = FilesHelper::getDataFiles($this->temp->getTmpFolder());
        self::assertCount(2, $slicedDirectories);

        $slicedDirectories = array_map(function (SplFileInfo $file) {
            return $file->getPathname();
        }, $slicedDirectories);
        sort($slicedDirectories);

        self::assertSame($originalSource->getFile()->getPathname(), $slicedDirectories[0]);
        self::assertCompressedSlicedData('"123","Test Name"' . PHP_EOL, $slicedDirectories[0]);

        self::assertSame($originalSlicedSource->getFile()->getPathname(), $slicedDirectories[1]);
        self::assertCompressedSlicedData('"123";"Test Name"' . PHP_EOL, $slicedDirectories[1]);

        self::assertCount(4, $this->logger->records);
        self::assertTrue($this->logger->hasInfo('Slicing table "test1.csv".'));
        self::assertTrue($this->logger->hasInfoThatContains('Table "test1.csv" sliced'));
        self::assertTrue($this->logger->hasInfo('Slicing table "test2.csv".'));
        self::assertTrue($this->logger->hasInfoThatContains('Table "test2.csv" sliced'));
    }

    public function testSliceSourcesIgnoresSliceSkippedExceptionsFromSlicer(): void
    {
        $originalMappingSource1 = new MappingSource(new WorkspaceItemSource(
            'table',
            '123',
            'test',
            false,
        ));
        $originalMappingSource1->setMapping(['delimiter' => ';']);

        $originalMappingSource2 = $this->createTestMappingSourceHavingManifest('test2.csv', $this->temp);
        /** @var LocalFileSource $originalSource2 */
        $originalSource2 = $originalMappingSource2->getSource();

        $originalMappingSources = [
            $originalMappingSource1,
            $originalMappingSource2,
        ];

        $mappingSources = (new SliceHelper($this->logger, '3b'))->sliceSources($originalMappingSources);
        self::assertCount(2, $mappingSources);

        // unmodified and immutable mapping source
        self::assertNotSame($originalMappingSource1, $mappingSources[0]);
        self::assertEquals($originalMappingSource1, $mappingSources[0]);
        self::assertNotSame($originalMappingSource1->getSource(), $mappingSources[0]->getSource());
        self::assertEquals($originalMappingSource1->getSource(), $mappingSources[0]->getSource());
        self::assertNull($mappingSources[0]->getManifestFile());
        self::assertEquals($originalMappingSource1->getManifestFile(), $mappingSources[0]->getManifestFile());

        // manifests data
        self::assertNotNull($mappingSources[1]->getManifestFile());
        $manifestFilePathName = $mappingSources[1]->getManifestFile()->getPathname();
        self::assertSame(
            $originalSource2->getFile()->getPathname() . '.manifest',
            $manifestFilePathName,
        );
        self::assertManifestData(
            [
                'delimiter' => ';',
                'columns' => ['id', 'name'],
            ],
            $manifestFilePathName,
        );

        // sources
        self::assertSource($originalSource2, $mappingSources[1]->getSource());

        $dataFiles = FilesHelper::getDataFiles($this->temp->getTmpFolder());
        self::assertCount(1, $dataFiles);

        $dataFiles = array_map(function (SplFileInfo $file) {
            return $file->getPathname();
        }, $dataFiles);
        sort($dataFiles);

        self::assertSame($originalSource2->getFile()->getPathname(), $dataFiles[0]);
        self::assertCompressedSlicedData('"123";"Test Name"' . PHP_EOL, $dataFiles[0]);
        self::assertCount(3, $this->logger->records);
        self::assertTrue($this->logger->hasWarning('Source "table" slicing skipped: Only local files '
            . 'are supported for slicing.'));
        self::assertTrue($this->logger->hasInfo('Slicing table "test2.csv".'));
        self::assertTrue($this->logger->hasInfoThatContains('Table "test2.csv" sliced'));
    }

    public function testSliceSourcesHavingSameSourceFileFailsWithInvalidOutputException(): void
    {
        $file = $this->temp->createFile('test.csv');
        $mappingSources = [
            new MappingSource(new LocalFileSource($file)),
            new MappingSource(new LocalFileSource($file)),
        ];

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Source "test.csv" has multiple destinations set.');

        (new SliceHelper($this->logger, '3b'))->sliceSources($mappingSources);
        self::assertCount(0, $this->logger->records);
    }

    private function createTestMappingSource(string $fileName, Temp $temp): MappingSource
    {
        $csvFile = $temp->createFile($fileName);
        file_put_contents($csvFile->getPathname(), '"id","name"' . PHP_EOL . '"123","Test Name"' . PHP_EOL);

        return new MappingSource(new LocalFileSource($csvFile));
    }

    private function createTestMappingSourceHavingMapping(string $fileName, Temp $temp, array $mapping): MappingSource
    {
        $mappingSource = $this->createTestMappingSource($fileName, $temp);
        $mappingSource->setMapping($mapping);
        return $mappingSource;
    }

    private function createTestMappingSourceHavingManifest(string $fileName, Temp $temp): MappingSource
    {
        $csvFile = $temp->createFile($fileName);
        file_put_contents($csvFile->getPathname(), '"123";"Test Name"' . PHP_EOL);

        $manifestFile = $temp->createFile($fileName . '.manifest');
        file_put_contents(
            $manifestFile->getPathname(),
            json_encode([
                'delimiter' => ';',
                'columns' => ['id', 'name'],
            ]),
        );

        return new MappingSource(
            new LocalFileSource($csvFile),
            (new SliceHelper(new NullLogger()))->getFile($manifestFile->getPathname()),
        );
    }

    private static function assertManifestData(array $expectedData, string $manifestPathName): void
    {
        self::assertFileExists($manifestPathName);
        self::assertSame(
            $expectedData,
            json_decode(
                (string) file_get_contents($manifestPathName),
                true,
            ),
        );
    }

    private static function assertCompressedSlicedData(string $expectedData, string $directoryPathName): void
    {
        self::assertDirectoryExists($directoryPathName);

        $slices = iterator_to_array((new Finder())->in($directoryPathName)->depth(0));
        self::assertCount(1, $slices);

        /** @var FinderSplFileInfo $slice */
        $slice = reset($slices);
        self::assertSame('part0001.gz', $slice->getFilename());
        self::assertSame($expectedData, gzdecode((string) file_get_contents($slice->getPathname())));
    }

    private static function assertSource(LocalFileSource $originalSource, SourceInterface $source): void
    {
        self::assertNotSame($originalSource, $source); // immutable source
        self::assertInstanceOf(LocalFileSource::class, $source);
        self::assertTrue($source->isSliced());
        self::assertSame(
            $originalSource->getFile()->getPathname(),
            $source->getFile()->getPathname(),
        );
    }

    public function testGetFile(): void
    {
        $temp = new Temp();

        $filePathName = $temp->getTmpFolder() . '/my.csv';
        touch($filePathName);
        self::assertSame($filePathName, (new SliceHelper(new NullLogger()))->getFile($filePathName)->getPathname());

        $directoryPathname = $temp->getTmpFolder() . '/sub-dir';
        mkdir($directoryPathname);
        try {
            (new SliceHelper(new NullLogger()))->getFile($directoryPathname);
            self::fail('getFile for directory path should fail');
        } catch (FileNotFoundException $e) {
            self::assertSame(
                sprintf('File "%s" could not be found.', $directoryPathname),
                $e->getMessage(),
            );
        }

        $filePathName = $temp->getTmpFolder() . '/dummy.csv';
        try {
            (new SliceHelper(new NullLogger()))->getFile($filePathName);
            self::fail('getFile for non-existing file should fail');
        } catch (FileNotFoundException $e) {
            self::assertSame(
                sprintf('File "%s" could not be found.', $filePathName),
                $e->getMessage(),
            );
        }
    }

    private function createTestGzippedMappingSource(string $fileName, Temp $temp): MappingSource
    {
        $csvFile = $temp->createFile($fileName . '.gz');
        file_put_contents(
            $csvFile->getPathname(),
            (string) gzencode(
                '"id","name"' . PHP_EOL . '"123","Test Name"' . PHP_EOL,
            ),
        );

        return new MappingSource(new LocalFileSource($csvFile));
    }
}
