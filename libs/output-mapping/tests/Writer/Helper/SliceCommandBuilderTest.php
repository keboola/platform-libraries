<?php

declare(strict_types=1);

namespace Keboola\OutputMapping\Tests\Writer\Helper;

use Keboola\OutputMapping\Writer\Helper\SliceCommandBuilder;
use Keboola\Slicer\Slicer;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use SplFileInfo;

class SliceCommandBuilderTest extends TestCase
{
    private Temp $temp;
    private SplFileInfo $testFile;
    public function setUp(): void
    {
        parent::setUp();

        $this->temp = new Temp();
        $this->testFile = $this->temp->createFile('data.csv');
    }

    public function testCreateProcess(): void
    {
        $process = SliceCommandBuilder::createProcess(
            $this->testFile->getBasename(),
            $this->testFile,
            new SplFileInfo($this->temp->getTmpFolder() . '/slicer-output-dir'),
        );

        self::assertSame(7200.0, $process->getTimeout());
        self::assertSame(
            implode(
                ' ',
                [
                    sprintf("'%s'", Slicer::getBinaryPath()),
                    sprintf("'--table-input-path=%s/data.csv'", $this->temp->getTmpFolder()),
                    "'--table-name=data.csv'",
                    sprintf("'--table-output-path=%s/slicer-output-dir'", $this->temp->getTmpFolder()),
                    sprintf(
                        "'--table-output-manifest-path=%s/slicer-output-dir.manifest'",
                        $this->temp->getTmpFolder(),
                    ),
                    "'--gzip=true'",
                    "'--input-size-low-exit-code=200'",
                ],
            ),
            $process->getCommandLine(),
        );
    }

    public function testCreateProcessWithManifest(): void
    {
        $process = SliceCommandBuilder::createProcess(
            $this->testFile->getBasename(),
            $this->testFile,
            new SplFileInfo($this->temp->getTmpFolder() . '/slicer-output-dir'),
            $this->temp->createFile('data.csv.manifest'),
        );

        self::assertSame(7200.0, $process->getTimeout());
        self::assertSame(
            implode(
                ' ',
                [
                    sprintf("'%s'", Slicer::getBinaryPath()),
                    sprintf("'--table-input-path=%s/data.csv'", $this->temp->getTmpFolder()),
                    "'--table-name=data.csv'",
                    sprintf("'--table-output-path=%s/slicer-output-dir'", $this->temp->getTmpFolder()),
                    sprintf(
                        "'--table-output-manifest-path=%s/slicer-output-dir.manifest'",
                        $this->temp->getTmpFolder(),
                    ),
                    "'--gzip=true'",
                    "'--input-size-low-exit-code=200'",
                    sprintf("'--table-input-manifest-path=%s/data.csv.manifest'", $this->temp->getTmpFolder()),
                ],
            ),
            $process->getCommandLine(),
        );
    }

    public function testCreateProcessWithInputThreshold(): void
    {
        $process = SliceCommandBuilder::createProcess(
            $this->testFile->getBasename(),
            $this->testFile,
            new SplFileInfo($this->temp->getTmpFolder() . '/slicer-output-dir'),
            null,
            '3GB',
        );

        self::assertSame(7200.0, $process->getTimeout());
        self::assertSame(
            implode(
                ' ',
                [
                    sprintf("'%s'", Slicer::getBinaryPath()),
                    sprintf("'--table-input-path=%s/data.csv'", $this->temp->getTmpFolder()),
                    "'--table-name=data.csv'",
                    sprintf("'--table-output-path=%s/slicer-output-dir'", $this->temp->getTmpFolder()),
                    sprintf(
                        "'--table-output-manifest-path=%s/slicer-output-dir.manifest'",
                        $this->temp->getTmpFolder(),
                    ),
                    "'--gzip=true'",
                    "'--input-size-low-exit-code=200'",
                    "'--input-size-threshold=3GB'",
                ],
            ),
            $process->getCommandLine(),
        );
    }
}
