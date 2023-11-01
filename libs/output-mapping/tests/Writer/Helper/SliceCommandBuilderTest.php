<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Writer\Helper\FilesHelper;
use Keboola\OutputMapping\Writer\Helper\SliceCommandBuilder;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use SplFileInfo;

class SliceCommandBuilderTest extends TestCase
{
    public function testCreate(): void
    {
        $temp = new Temp();

        self::assertSame(
            [
                './bin/slicer',
                sprintf('--table-input-path=%s/data.csv', $temp->getTmpFolder()),
                '--table-name=LOG_PLACEHOLDER',
                sprintf('--table-output-path=%s/slicer-output-dir', $temp->getTmpFolder()),
                sprintf('--table-output-manifest-path=%s/slicer-output-dir.manifest', $temp->getTmpFolder()),
                '--gzip=false',
            ],
            SliceCommandBuilder::create(
                $temp->createFile('data.csv'),
                new SplFileInfo($temp->getTmpFolder() . '/slicer-output-dir'),
            ),
        );
    }

    public function testCreateWithManifest(): void
    {
        $temp = new Temp();

        self::assertSame(
            [
                './bin/slicer',
                sprintf('--table-input-path=%s/data.csv', $temp->getTmpFolder()),
                '--table-name=LOG_PLACEHOLDER',
                sprintf('--table-output-path=%s/slicer-output-dir', $temp->getTmpFolder()),
                sprintf('--table-output-manifest-path=%s/slicer-output-dir.manifest', $temp->getTmpFolder()),
                '--gzip=false',
                sprintf('--table-input-manifest-path=%s/data.csv.manifest', $temp->getTmpFolder()),
            ],
            SliceCommandBuilder::create(
                $temp->createFile('data.csv'),
                new SplFileInfo($temp->getTmpFolder() . '/slicer-output-dir'),
                $temp->createFile('data.csv.manifest'),
            ),
        );
    }
}
