<?php

declare(strict_types=1);

namespace Keboola\Slicer\Tests;

use Composer\Composer;
use Composer\Config;
use Composer\Script\Event;
use Keboola\Slicer\Slicer;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;

class SlicerTest extends TestCase
{
    public function testInstallSlicer(): void
    {
        $configMock = $this->createMock(Config::class);
        $configMock->expects(self::once())
            ->method('get')
            ->with('bin-dir')
            ->willReturn(DownloaderTest::BIN_DIRECTORY_PATH)
        ;

        $composerMock = $this->createMock(Composer::class);
        $composerMock->expects(self::once())
            ->method('getConfig')
            ->willReturn($configMock)
        ;

        $eventMock = $this->createMock(Event::class);
        $eventMock->expects(self::once())
            ->method('getComposer')
            ->willReturn($composerMock)
        ;

        Slicer::installSlicer($eventMock);
        self::assertFileExists(DownloaderTest::BIN_DIRECTORY_PATH . '/slicer');
        $fs = new Filesystem();
        $fs->remove(DownloaderTest::BIN_DIRECTORY_PATH . '/slicer');
    }
}
