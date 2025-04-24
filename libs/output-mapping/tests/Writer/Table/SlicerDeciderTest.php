<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Table;

use Keboola\OutputMapping\Exception\InvalidOutputException;
use Keboola\OutputMapping\Mapping\MappingFromRawConfiguration;
use Keboola\OutputMapping\Mapping\MappingFromRawConfigurationAndPhysicalDataWithManifest;
use Keboola\OutputMapping\Writer\Table\SlicerDecider;
use Keboola\Temp\Temp;
use Monolog\Handler\TestHandler;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;

class SlicerDeciderTest extends TestCase
{
    private Temp $temp;

    protected function setUp(): void
    {
        $this->temp = new Temp();
        $this->temp->createFile('file1.csv');
        file_put_contents($this->temp->getTmpFolder() . '/file1.csv', 'file1 content');
        $this->temp->createFile('file2.csv');
        file_put_contents($this->temp->getTmpFolder() . '/file2.csv', 'file2 content');
        $this->temp->createFile('empty-file.csv');
    }

    public function testDecideSliceMultipleFiles(): void
    {
        $mock1 = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $mock1->method('getSourceName')->willReturn('file1');
        $mock1->method('isSliced')->willReturn(false);
        $mock1->method('getPathName')->willReturn($this->temp->getTmpFolder() . '/file1.csv');
        $mock1->method('getConfiguration')->willReturn(null);

        $mock2 = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $mock2->method('getSourceName')->willReturn('file2');
        $mock2->method('isSliced')->willReturn(false);
        $mock2->method('getPathName')->willReturn($this->temp->getTmpFolder() . '/file2.csv');
        $mock2->method('getConfiguration')->willReturn(null);

        $combinedSources = [
            $mock1,
            $mock2,
        ];

        $logger = new Logger('test');
        $slicerDecider = new SlicerDecider($logger);
        $result = $slicerDecider->decideSliceFiles($combinedSources);

        $this->assertCount(2, $result);
    }

    public function testDecideSliceEmptyFile(): void
    {
        $mock1 = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $mock1->method('getSourceName')->willReturn('empty-file');
        $mock1->method('isSliced')->willReturn(false);
        $mock1->method('getPathName')->willReturn($this->temp->getTmpFolder() . '/empty-file.csv');
        $mock1->method('getConfiguration')->willReturn(null);

        $combinedSources = [
            $mock1,
        ];

        $logger = new Logger('test');
        $slicerDecider = new SlicerDecider($logger);
        $result = $slicerDecider->decideSliceFiles($combinedSources);

        $this->assertCount(0, $result);
    }

    public function testDecideSliceTwoSameFiles(): void
    {
        $mock1 = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $mock1->method('getSourceName')->willReturn('file1');
        $mock1->method('isSliced')->willReturn(false);
        $mock1->method('getPathName')->willReturn($this->temp->getTmpFolder() . '/file1.csv');
        $mock1->method('getConfiguration')->willReturn(null);

        $combinedSources = [
            $mock1,
            $mock1,
        ];

        $testLogger = new Logger('test');
        $slicerDecider = new SlicerDecider($testLogger);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage('Source "file1" has multiple destinations set.');
        $slicerDecider->decideSliceFiles($combinedSources);
    }

    public function testDecideSliceSlicedFilesWithoutManifest(): void
    {
        $mock1 = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $mock1->method('getSourceName')->willReturn('file1');
        $mock1->method('isSliced')->willReturn(true);
        $mock1->method('getPathName')->willReturn($this->temp->getTmpFolder() . '/file1.csv');
        $mock1->method('getConfiguration')->willReturn(null);
        $mock1->method('getManifest')->willReturn(null);

        $combinedSources = [
            $mock1,
        ];

        $logsHandler = new TestHandler();
        $logger = new Logger('test', [$logsHandler]);
        $slicerDecider = new SlicerDecider($logger);

        $result = $slicerDecider->decideSliceFiles($combinedSources);

        self::assertCount(0, $result);
        self::assertTrue($logsHandler->hasWarningThatContains(
            'Sliced files without manifest are not supported. Skipping file "file1"',
        ));
    }

    public function testDecideFileWithUnsuportedConfigurationMapping(): void
    {
        $mock1 = $this->createMock(MappingFromRawConfigurationAndPhysicalDataWithManifest::class);
        $mock1->method('getSourceName')->willReturn('file1');
        $mock1->method('isSliced')->willReturn(false);
        $mock1->method('getPathName')->willReturn($this->temp->getTmpFolder() . '/file1.csv');
        $mock1->method('getConfiguration')->willReturn(
            new MappingFromRawConfiguration(['source' => 'file1', 'enclosure' => 'abcd']),
        );

        $combinedSources = [
            $mock1,
        ];

        $logger = new Logger('test');
        ;
        $slicerDecider = new SlicerDecider($logger);

        $this->expectException(InvalidOutputException::class);
        $this->expectExceptionMessage(
            'Params "delimiter", "enclosure" or "columns" specified in mapping are not longer supported.' .
            ' Skipping file "file1".',
        );
        $slicerDecider->decideSliceFiles($combinedSources);
    }
}
