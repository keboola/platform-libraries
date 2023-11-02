<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use InvalidArgumentException;
use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\OutputMapping\Writer\Helper\SliceHelper;
use Keboola\OutputMapping\Writer\Table\MappingSource;
use Keboola\OutputMapping\Writer\Table\Source\LocalFileSource;
use Keboola\OutputMapping\Writer\Table\Source\WorkspaceItemSource;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use SplFileInfo;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo as FinderSplFileInfo;

class SliceHelperTest extends TestCase
{
    public function testSliceWorkspaceSourceIsNotSupported(): void
    {
        $source = new WorkspaceItemSource(
            'dummy',
            '123',
            'test',
            false,
        );

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Only LocalFileSource is supported.');
        SliceHelper::sliceFile(new MappingSource($source));
    }

    public function testSliceSlicedSourceIsNotSupported(): void
    {
        $temp = new Temp();
        $source = new LocalFileSource(new SplFileInfo($temp->getTmpFolder()));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sliced files are not yet supported.');
        SliceHelper::sliceFile(new MappingSource($source));
    }

    public function testSlice(): void
    {
        $temp = new Temp();

        $csvFile = $temp->createFile('test.csv');
        file_put_contents($csvFile->getPathname(), '"id","name"' . PHP_EOL . '"123","Test Name"');

        $source = new LocalFileSource($csvFile);

        $mappingSource = SliceHelper::sliceFile(new MappingSource($source));

        $expectedManifestFilePathname = $source->getFile()->getPathname() . '.manifest';

        self::assertSame($source, $mappingSource->getSource());
        self::assertNotNull($mappingSource->getManifestFile());

        $manifestFilePathName = $mappingSource->getManifestFile()->getPathname();
        self::assertSame($expectedManifestFilePathname, $manifestFilePathName);
        self::assertFileExists($manifestFilePathName);
        self::assertSame(
            [
                'columns' => ['id', 'name'],
            ],
            json_decode(
                (string) file_get_contents($manifestFilePathName),
                true,
            ),
        );

        $dataFiles = FilesHelper::getDataFiles($temp->getTmpFolder());
        self::assertCount(1, $dataFiles);

        /** @var FinderSplFileInfo $slicedDirectory */
        $slicedDirectory = array_shift($dataFiles);
        self::assertTrue($slicedDirectory->isDir());
        self::assertSame($csvFile->getPathname(), $slicedDirectory->getPathname());

        $slices = iterator_to_array((new Finder())->in($slicedDirectory->getPathname())->depth(0));
        self::assertCount(1, $slices);

        /** @var FinderSplFileInfo $slice */
        $slice = reset($slices);
        self::assertSame('part0001', $slice->getFilename());
        self::assertSame('"123","Test Name"', file_get_contents($slice->getPathname()));
    }

    public function testSliceWithManifest(): void
    {
        self::markTestIncomplete('Not implemented');
    }
}
