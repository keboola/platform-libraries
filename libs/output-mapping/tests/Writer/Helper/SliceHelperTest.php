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
use SplFileInfo;
use Symfony\Component\Filesystem\Exception\FileNotFoundException;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo as FinderSplFileInfo;

class SliceHelperTest extends TestCase
{
    private readonly Temp $temp;

    public function setUp(): void
    {
        parent::setUp();

        $this->temp = new Temp();
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
        $this->expectExceptionMessage('Only local files is supported for slicing.');
        SliceHelper::sliceFile(new MappingSource($source));
    }

    public function testSliceSlicedSourceIsNotSupported(): void
    {
        $source = new LocalFileSource(new SplFileInfo($this->temp->getTmpFolder()));

        $this->expectException(SliceSkippedException::class);
        $this->expectExceptionMessage('Sliced files are not yet supported.');
        SliceHelper::sliceFile(new MappingSource($source));
    }

    public function testSliceEmptyFileSourceIsNotSupported(): void
    {
        $this->expectException(SliceSkippedException::class);
        $this->expectExceptionMessage('Empty files cannot be sliced.');

        SliceHelper::sliceFile(
            new MappingSource(
                new LocalFileSource(
                    (new Temp())->createFile('test.csv'),
                ),
            ),
        );
    }

    public function sliceSourceWithSomeMappingOptionsIsNotSupportedProvider(): Generator
    {
        yield 'mapping with csv options - delimiter' => [
            'mapping' => ['delimiter' => ';'],
            'expectedErrorMessage' => 'Params "delimiter" or "enclosure"' .
                ' specified in mapping are not supported by slicer.',
        ];
        yield 'mapping with csv options - enclosure' => [
            'mapping' => ['enclosure' => '"'],
            'expectedErrorMessage' => 'Params "delimiter" or "enclosure"' .
                ' specified in mapping are not supported by slicer.',
        ];
        yield 'mapping with columns' => [
            'mapping' => ['columns' => ['Id']],
            'expectedErrorMessage' => 'Param "columns" specified in mapping is not supported by slicer.',
        ];
    }

    /**
     * @dataProvider sliceSourceWithSomeMappingOptionsIsNotSupportedProvider
     */
    public function testSliceSourceWithMappingHavingCsvOptionsIsNotSupported(
        array $mapping,
        string $expectedErrorMessage,
    ): void {
        $file = $this->temp->createFile('test.csv');
        file_put_contents($file->getPathname(), '"id","name"');

        $mappingSource = new MappingSource(
            new LocalFileSource($file),
        );
        $mappingSource->setMapping($mapping);

        $this->expectException(SliceSkippedException::class);
        $this->expectExceptionMessage($expectedErrorMessage);

        SliceHelper::sliceFile($mappingSource);
    }

    public function sliceProvider(): Generator
    {
        yield 'file without manifest' => [
            'originalMappingSource' => $this->createTestMappingSource('test1.csv', new Temp()),
            'expectedData' => '"123","Test Name"',
            'expectedManifestData' => [
                'columns' => ['id', 'name'],
            ],
        ];
        yield 'file with manifest' => [
            'originalMappingSource' => $this->createTestMappingSourceHavingManifest('test2.csv', new Temp()),
            'expectedData' => '"123";"Test Name"',
            'expectedManifestData' => [
                'delimiter' => ';',
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
        $mappingSource = SliceHelper::sliceFile($originalMappingSource);

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
        self::assertSlicedData($expectedData, $slicedDirectory->getPathname());
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

        $mappingSources = SliceHelper::sliceSources($originalMappingSources);
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
        self::assertSlicedData('"123","Test Name"', $slicedDirectories[0]);

        self::assertSame($originalSlicedSource->getFile()->getPathname(), $slicedDirectories[1]);
        self::assertSlicedData('"123";"Test Name"', $slicedDirectories[1]);
    }

    public function testSliceSourcesIgnoresSliceSkippedExceptionsFromSlicer(): void
    {
        $originalMappingSource1 = $this->createTestMappingSourceHavingManifest('test1.csv', $this->temp);
        $originalMappingSource1->setMapping(['delimiter' => ';']);

        /** @var LocalFileSource $originalSource1 */
        $originalSource1 = $originalMappingSource1->getSource();

        $originalMappingSource2 = $this->createTestMappingSourceHavingManifest('test2.csv', $this->temp);
        /** @var LocalFileSource $originalSource2 */
        $originalSource2 = $originalMappingSource2->getSource();

        $originalMappingSources = [
            $originalMappingSource1,
            $originalMappingSource2,
        ];

        $mappingSources = SliceHelper::sliceSources($originalMappingSources);
        self::assertCount(2, $mappingSources);

        // unmodified and immutable mapping source
        self::assertNotSame($originalMappingSource1, $mappingSources[0]);
        self::assertEquals($originalMappingSource1, $mappingSources[0]);
        self::assertNotSame($originalMappingSource1->getSource(), $mappingSources[0]->getSource());
        self::assertEquals($originalMappingSource1->getSource(), $mappingSources[0]->getSource());
        self::assertNotSame($originalMappingSource1->getManifestFile(), $mappingSources[0]->getManifestFile());
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
        self::assertCount(2, $dataFiles);

        $dataFiles = array_map(function (SplFileInfo $file) {
            return $file->getPathname();
        }, $dataFiles);
        sort($dataFiles);

        self::assertFileEquals($originalSource1->getFile()->getPathname(), $dataFiles[0]);

        self::assertSame($originalSource2->getFile()->getPathname(), $dataFiles[1]);
        self::assertSlicedData('"123";"Test Name"', $dataFiles[1]);
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

        SliceHelper::sliceSources($mappingSources);
    }

    private function createTestMappingSource(string $fileName, Temp $temp): MappingSource
    {
        $csvFile = $temp->createFile($fileName);
        file_put_contents($csvFile->getPathname(), '"id","name"' . PHP_EOL . '"123","Test Name"');

        return new MappingSource(new LocalFileSource($csvFile));
    }

    private function createTestMappingSourceHavingManifest(string $fileName, Temp $temp): MappingSource
    {
        $csvFile = $temp->createFile($fileName);
        file_put_contents($csvFile->getPathname(), '"123";"Test Name"');

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
            SliceHelper::getFile($manifestFile->getPathname()),
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

    private static function assertSlicedData(string $expectedData, string $directoryPathName): void
    {
        self::assertDirectoryExists($directoryPathName);

        $slices = iterator_to_array((new Finder())->in($directoryPathName)->depth(0));
        self::assertCount(1, $slices);

        /** @var FinderSplFileInfo $slice */
        $slice = reset($slices);
        self::assertSame('part0001', $slice->getFilename());
        self::assertSame($expectedData, file_get_contents($slice->getPathname()));
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
        self::assertSame($filePathName, SliceHelper::getFile($filePathName)->getPathname());

        $directoryPathname = $temp->getTmpFolder() . '/sub-dir';
        mkdir($directoryPathname);
        try {
            SliceHelper::getFile($directoryPathname);
            self::fail('getFile for directory path should fail');
        } catch (FileNotFoundException $e) {
            self::assertSame(
                sprintf('File "%s" could not be found.', $directoryPathname),
                $e->getMessage(),
            );
        }

        $filePathName = $temp->getTmpFolder() . '/dummy.csv';
        try {
            SliceHelper::getFile($filePathName);
            self::fail('getFile for non-existing file should fail');
        } catch (FileNotFoundException $e) {
            self::assertSame(
                sprintf('File "%s" could not be found.', $filePathName),
                $e->getMessage(),
            );
        }
    }
}
