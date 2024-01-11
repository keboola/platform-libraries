<?php

declare(strict_types=1);

namespace Keboola\Slicer\Tests;

use Keboola\Slicer\Slicer;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Filesystem\Filesystem;

class SlicerTest extends TestCase
{
    public function testInstallSlicer(): void
    {
        $fs = new Filesystem();
        $fs->remove(Slicer::getBinaryPath());

        Slicer::installSlicer();
        self::assertFileExists(Slicer::getBinaryPath());
        $fs = new Filesystem();
        $fs->remove(Slicer::getBinaryPath());
    }
}
